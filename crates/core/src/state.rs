use std::{collections::HashMap, marker::PhantomData, ops::Range};

use crate::{BlockSlot, TxoRef};

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

    pub fn iter_deltas(&self) -> impl Iterator<Item = (Namespace, &[EntityDelta])> {
        self.entries
            .iter()
            .map(|(ns, deltas)| (*ns, deltas.as_slice()))
    }

    pub fn get_overriden_key(&self, ns: Namespace, key: impl AsRef<[u8]>) -> Option<&EntityValue> {
        let key = key.as_ref().to_vec();
        let deltas = self.entries.get(ns)?;

        let delta = deltas
            .iter()
            .rev()
            .find(|delta| matches!(delta, EntityDelta::OverrideKey(k, _, _) if k == &key))?;

        match delta {
            EntityDelta::OverrideKey(_, value, _) => Some(value),
            _ => None,
        }
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

    pub fn append_entity<T: Entity>(&mut self, key: impl Into<EntityKey>, entity: T) {
        self.append_value(T::NS, key, entity.encode_value());
    }
}

#[derive(Debug, Clone, Default)]
pub struct NamespaceSlice {
    pub entities: HashMap<EntityKey, EntityValue>,
}

#[derive(Debug, Clone, Default)]
pub struct StateSlice {
    pub loaded: HashMap<Namespace, NamespaceSlice>,
}

impl StateSlice {
    pub fn load_entity(&mut self, ns: Namespace, key: impl AsRef<[u8]>, value: EntityValue) {
        let key = key.as_ref().to_vec();

        self.loaded
            .entry(ns)
            .or_default()
            .entities
            .entry(key)
            .or_insert(value);
    }

    pub fn load_entity_typed<T: Entity>(&mut self, key: impl AsRef<[u8]>, entity: T) {
        let value = entity.encode_value();

        self.load_entity(T::NS, key, value);
    }

    pub fn get_entity(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<EntityValue>, StateError> {
        let values = self
            .loaded
            .get(ns)
            .and_then(|ns| ns.entities.get(key.as_ref()));

        let value = values.cloned();

        Ok(value)
    }

    pub fn get_entity_typed<T: Entity>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>, StateError> {
        let value = self.get_entity(T::NS, key)?;

        // TODO: we need to optimize this so that we don't repeat decoding if this is
        // called multiple times.
        let decoded = value.map(T::decode_value).transpose()?;

        Ok(decoded)
    }
}

pub struct StateSliceView<'a> {
    inner: StateSlice,
    deltas: &'a [StateDelta],
}

impl<'a> StateSliceView<'a> {
    pub fn new(inner: StateSlice, deltas: &'a [StateDelta]) -> Self {
        Self { inner, deltas }
    }

    /// Looks for an entity overridden in the deltas.
    ///
    /// we look for the entity in the deltas in reverse order, so that the
    /// latest delta is searched first
    fn find_override(&self, ns: Namespace, key: impl AsRef<[u8]>) -> Option<&EntityValue> {
        for delta in self.deltas.iter().rev() {
            if let Some(delta) = delta.get_overriden_key(ns, &key) {
                return Some(delta);
            }
        }

        None
    }

    pub fn ensure_loaded(
        &mut self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
        store: &impl State3Store,
    ) -> Result<(), State3Error> {
        let key = key.as_ref().to_vec();

        if self.find_override(ns, &key).is_some() {
            return Ok(());
        }

        let value = store.read_entity(ns, &key)?;

        if let Some(value) = value {
            self.inner.load_entity(ns, &key, value);
        }

        Ok(())
    }

    pub fn ensure_loaded_typed<T: Entity>(
        &mut self,
        key: impl AsRef<[u8]>,
        store: &impl State3Store,
    ) -> Result<(), State3Error> {
        self.ensure_loaded(T::NS, key, store)
    }

    pub fn get_entity(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<EntityValue>, StateError> {
        let key = key.as_ref().to_vec();

        if let Some(delta) = self.find_override(ns, &key) {
            return Ok(Some(delta.clone()));
        }

        let value = self.inner.get_entity(ns, key)?;

        Ok(value)
    }

    pub fn get_entity_typed<T: Entity>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>, StateError> {
        let value = self.get_entity(T::NS, key)?;
        let decoded = value.map(T::decode_value).transpose()?;

        Ok(decoded)
    }

    pub fn unwrap(self) -> StateSlice {
        self.inner
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvariantViolation {
    #[error("input not found: {0}")]
    InputNotFound(TxoRef),
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

    #[error(transparent)]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error(transparent)]
    InvariantViolation(#[from] InvariantViolation),
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

    fn apply(&self, deltas: &[StateDelta]) -> Result<(), StateError>;

    fn undo(&self, deltas: &[StateDelta]) -> Result<(), StateError>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestEntity {
        value: String,
    }

    impl TestEntity {
        pub fn new(value: &str) -> Self {
            Self {
                value: value.to_string(),
            }
        }
    }

    impl Entity for TestEntity {
        const NS: Namespace = "test";
        const NS_TYPE: NamespaceType = NamespaceType::KeyValue;

        fn decode_value(value: EntityValue) -> Result<Self, StateError> {
            let value_str =
                String::from_utf8(value).map_err(|e| StateError::EncodingError(e.to_string()))?;
            Ok(TestEntity { value: value_str })
        }

        fn encode_value(self) -> EntityValue {
            self.value.into_bytes()
        }
    }

    #[test]
    fn test_state_slice_view() {
        // Create a base state slice with an entity
        let mut base_slice = StateSlice::default();

        let base_entity = TestEntity::new("a");
        base_slice.load_entity_typed(b"overriden_key", base_entity.clone());

        let base_entity_2 = TestEntity::new("a");
        base_slice.load_entity_typed(b"not_overriden_key", base_entity_2.clone());

        // Create a delta with a different value for the same key
        let mut delta = StateDelta::new(1);

        let delta_entity = TestEntity::new("b");
        delta.override_entity(b"overriden_key", delta_entity, Some(base_entity.clone()));

        // Create a state slice view with the base slice and delta
        let deltas = vec![delta];
        let view = StateSliceView::new(base_slice.clone(), &deltas);

        // Test that get_entity returns the delta value, not the base value
        let found = view
            .get_entity_typed::<TestEntity>(b"overriden_key")
            .unwrap()
            .unwrap();

        // Should return the delta value, not the base value
        assert_eq!(found.value, "b");

        // Test that get_entity returns the base value when no delta exists
        let found = view
            .get_entity_typed::<TestEntity>(b"not_overriden_key")
            .unwrap()
            .unwrap();

        assert_eq!(found.value, "a");
    }
}
