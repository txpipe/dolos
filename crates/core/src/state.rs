use std::{borrow::Cow, collections::HashMap, marker::PhantomData, ops::Range};

use itertools::Itertools;

use crate::{BlockSlot, Domain, TxoRef};

const KEY_SIZE: usize = 32;

pub type Namespace = &'static str;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct EntityKey([u8; KEY_SIZE]);

impl<const N: usize> From<&[u8; N]> for EntityKey {
    fn from(value: &[u8; N]) -> Self {
        value.into()
    }
}

impl From<&[u8]> for EntityKey {
    fn from(value: &[u8]) -> Self {
        let mut key = [0u8; KEY_SIZE];
        let len = value.len().min(KEY_SIZE);
        key[..len].copy_from_slice(&value[..len]);
        EntityKey(key)
    }
}

impl From<Vec<u8>> for EntityKey {
    fn from(value: Vec<u8>) -> Self {
        value.as_slice().into()
    }
}

impl std::fmt::Display for EntityKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl AsRef<[u8]> for EntityKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A namespaced key
///
/// Represent a key to an entity by also specifying the namespace to which it
/// belongs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct NsKey(Namespace, EntityKey);

impl<T> From<(&'static str, T)> for NsKey
where
    T: Into<EntityKey>,
{
    fn from((ns, key): (&'static str, T)) -> Self {
        Self(ns, key.into())
    }
}

impl std::fmt::Display for NsKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl AsRef<EntityKey> for NsKey {
    fn as_ref(&self) -> &EntityKey {
        &self.1
    }
}

pub type EntityMap<E> = HashMap<NsKey, Option<E>>;

pub type EntityValue = Vec<u8>;

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

pub trait Entity: Sized + Send {
    const KEY_SIZE: usize = 32;

    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, StateError>;
    fn encode_entity(value: &Self) -> (Namespace, EntityValue);
}

pub type KeyEntityPair<E> = (EntityKey, Option<E>);

pub trait EntityDelta {
    type Entity: Entity;

    fn key(&self) -> Cow<'_, NsKey>;

    /// Applies the change to the entity
    ///
    /// Implementing types will take an entity representing the latest known
    /// state and apply any changes declared by the delta.
    ///
    /// Returning `Some` instructs the machinery to upsert the entity in the
    /// database; returning `None` instructs the machinery to delete the record.
    ///
    /// Implementors must also use this call to store any required data from the
    /// entity for a potential `undo` call at a later time. Eg: if the apply
    /// erases a value, this method should store internally a copy of that value
    /// in case it needs to re-assign the field during an undo.
    fn apply(&mut self, entity: &mut Option<Self::Entity>);

    /// Undo the changes to the entity
    ///
    /// Implementing types will take the entity with changes already applied and
    /// undo those updates to reset the entity to the previous state.
    ///
    /// This method should assume that `apply` was already called at a prior
    /// point in time, allowing implementors to retain initial values as
    /// internal delta state (if required).
    fn undo(&mut self, entity: &mut Option<Self::Entity>);
}

#[derive(Debug, Clone)]
pub struct StateDelta<D> {
    pub(crate) new_cursor: BlockSlot,
    pub(crate) deltas: HashMap<NsKey, Vec<D>>,
}

impl<D> Default for StateDelta<D> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<D> std::ops::AddAssign<Self> for StateDelta<D> {
    fn add_assign(&mut self, rhs: Self) {
        for (key, deltas) in rhs.deltas {
            let entry = self.deltas.entry(key).or_default();
            entry.extend(deltas);
        }

        self.new_cursor = self.new_cursor.max(rhs.new_cursor);
    }
}

impl<D> StateDelta<D> {
    fn new(new_cursor: BlockSlot) -> Self {
        Self {
            new_cursor,
            deltas: HashMap::new(),
        }
    }

    pub fn set_cursor(&mut self, value: BlockSlot) {
        self.new_cursor = value;
    }
}

impl<D> StateDelta<D>
where
    D: EntityDelta,
{
    pub fn add_delta(&mut self, delta: impl Into<D>) {
        let delta = delta.into();
        let key = delta.key();
        let group = self.deltas.entry(key.into_owned()).or_default();
        group.push(delta);
    }

    pub fn compile_keys(&self) -> impl Iterator<Item = &NsKey> {
        self.deltas.keys()
    }

    fn apply_to(&mut self, key: &NsKey, entity: &mut Option<D::Entity>) {
        let to_apply = self.deltas.get_mut(key);

        if let Some(to_apply) = to_apply {
            for delta in to_apply {
                delta.apply(entity);
            }
        }
    }

    fn undo_to(&mut self, key: &NsKey, entity: &mut Option<D::Entity>) {
        let to_apply = self.deltas.get_mut(key);

        if let Some(to_apply) = to_apply {
            for delta in to_apply {
                delta.undo(entity);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvariantViolation {
    #[error("input not found: {0}")]
    InputNotFound(TxoRef),

    #[error("entity not found: {0}")]
    EntityNotFound(NsKey),
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

    #[error("invalid namespace: {0}")]
    InvalidNamespace(Namespace),

    #[error(transparent)]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error(transparent)]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error(transparent)]
    InvariantViolation(#[from] InvariantViolation),
}

// temporary alias to avoid collision with existing StateError
pub type State3Error = StateError;

pub struct EntityIterTyped<S: State3Store, E: Entity> {
    inner: S::EntityIter,
    ns: Namespace,
    _marker: PhantomData<E>,
}

impl<S: State3Store, E: Entity> EntityIterTyped<S, E> {
    pub fn new(inner: S::EntityIter, ns: Namespace) -> Self {
        Self {
            inner,
            ns,
            _marker: PhantomData,
        }
    }
}

impl<S: State3Store, E: Entity> Iterator for EntityIterTyped<S, E> {
    type Item = Result<(EntityKey, E), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped =
            next.and_then(|(key, value)| E::decode_entity(&self.ns, &value).map(|v| (key, v)));

        Some(mapped)
    }
}

// pub struct EntityValueIterTyped<S: State3Store> {
//     inner: S::EntityValueIter,
//     ns: Namespace,
//     _marker: PhantomData<S::Entity>,
// }

// impl<S: State3Store> EntityValueIterTyped<S> {
//     pub fn new(inner: S::EntityValueIter, ns: Namespace) -> Self {
//         Self {
//             inner,
//             ns,
//             _marker: PhantomData,
//         }
//     }
// }

// impl<S: State3Store> Iterator for EntityValueIterTyped<S> {
//     type Item = Result<S::Entity, StateError>;

//     fn next(&mut self) -> Option<Self::Item> {
//         let next = self.inner.next()?;

//         let mapped = next.and_then(|value| S::Entity::decode_entity(&self.ns,
// &value));

//         Some(mapped)
//     }
// }

fn full_range() -> Range<EntityKey> {
    let start = [0u8; KEY_SIZE];
    let end = [255u8; KEY_SIZE];
    Range {
        start: EntityKey(start),
        end: EntityKey(end),
    }
}

pub trait State3Store: Sized + Send + Sync {
    type EntityIter: Iterator<Item = Result<(EntityKey, EntityValue), StateError>>;
    type EntityValueIter: Iterator<Item = Result<EntityValue, StateError>>;

    fn read_cursor(&self) -> Result<Option<BlockSlot>, StateError>;

    fn append_cursor(&self, cursor: BlockSlot) -> Result<(), StateError>;

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError>;

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError>;

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError>;

    fn save_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        maybe_entity: Option<&EntityValue>,
    ) -> Result<(), StateError> {
        if let Some(entity) = maybe_entity {
            self.write_entity(ns, key, &entity)
        } else {
            self.delete_entity(ns, key)
        }
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError>;

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError>;

    fn read_entities_typed<E: Entity>(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<E>>, StateError> {
        let raw = self.read_entities(ns, keys)?;

        let decoded = raw
            .into_iter()
            .map(|x| x.map(|v| E::decode_entity(ns, &v)))
            .map(|x| x.transpose())
            .try_collect()?;

        Ok(decoded)
    }

    fn read_entity_typed<E: Entity>(
        &self,
        ns: Namespace,
        key: &EntityKey,
    ) -> Result<Option<E>, StateError> {
        let raw = self.read_entities_typed(ns, &[key])?;

        let first = raw.into_iter().next().unwrap();

        Ok(first)
    }

    fn write_entity_typed<E: Entity>(&self, key: &EntityKey, entity: &E) -> Result<(), StateError> {
        let (ns, raw) = E::encode_entity(&entity);

        self.write_entity(ns, key, &raw)
    }

    fn save_entity_typed<E: Entity>(
        &self,
        ns: Namespace,
        key: &EntityKey,
        maybe_entity: Option<&E>,
    ) -> Result<(), StateError> {
        if let Some(entity) = maybe_entity {
            self.write_entity_typed(key, entity)
        } else {
            self.delete_entity(ns, key)
        }
    }

    fn iter_entities_typed<E: Entity>(
        &self,
        ns: Namespace,
        range: Option<Range<EntityKey>>,
    ) -> Result<EntityIterTyped<Self, E>, StateError> {
        let range = range.unwrap_or_else(|| full_range());

        let inner = self.iter_entities(ns, range)?;

        Ok(EntityIterTyped::<Self, E>::new(inner, ns))
    }

    // fn iter_entity_values_typed<E: Entity>(
    //     &self,
    //     ns: Namespace,
    //     key: impl AsRef<[u8]>,
    // ) -> Result<EntityValueIterTyped<E>, StateError> {
    //     let inner = self.iter_entity_values(ns, key)?;
    //     Ok(EntityValueIterTyped::<E>::new(inner, ns))
    // }
}

use rayon::prelude::*;

fn load_entity_chunk<D: Domain>(
    chunk: &[NsKey],
    store: &D::State3,
) -> Result<EntityMap<D::Entity>, StateError> {
    let by_ns = chunk.into_iter().chunk_by(|key| key.0);

    let mut loaded: EntityMap<D::Entity> = HashMap::new();

    for (ns, chunk) in &by_ns {
        let keys = chunk.map(|x| &x.1).collect::<Vec<_>>();

        let decoded = store.read_entities_typed::<D::Entity>(ns, &keys)?;

        loaded = keys
            .into_iter()
            .zip(decoded)
            .fold(loaded, |mut acc, (k, v)| {
                let k = NsKey(ns, k.clone());
                acc.insert(k, v);
                acc
            });
    }

    Ok(loaded)
}

const LOAD_CHUNK_SIZE: usize = 100;

/// Loads the entities involved in a batch of deltas
///
/// This methods is a fancy way of loading required entities for a batch of
/// deltas. It optimizes the process by organizing read operations in chunks
/// that execute in parallel using Rayon. The assumption is that the storage
/// backend supports concurrent reads (eg: Redb).
///
/// Chunks are defined by sorting the entity keys grouping by namespace. The
/// assumption is that the storage backend will benefit from loading keys that
/// are close to each other (eg: disk block reads)
fn load_entities<D: Domain>(
    store: &D::State3,
    delta: &StateDelta<D::EntityDelta>,
) -> Result<EntityMap<D::Entity>, StateError> {
    let mut keys: Vec<_> = delta.compile_keys().cloned().collect();

    keys.sort();

    let result = keys
        .par_chunks(LOAD_CHUNK_SIZE)
        .map(|chunk| load_entity_chunk::<D>(chunk, store))
        .try_reduce(
            || EntityMap::new(),
            |mut acc, x| {
                acc.extend(x);
                Ok(acc)
            },
        )?;

    Ok(result)
}

pub fn apply_batch<D: Domain>(
    store: &D::State3,
    delta: &mut StateDelta<D::EntityDelta>,
) -> Result<(), StateError> {
    // todo: semantics for starting a read transaction

    let mut entities = load_entities::<D>(store, delta)?;

    for (key, entity) in entities.iter_mut() {
        delta.apply_to(key, entity);
    }

    // lets keep this as a separated loop because we might want to isolate the write
    // phase as a different method for pipelining
    for (key, entity) in entities {
        let NsKey(ns, key) = key;
        store.save_entity_typed(ns, &key, entity.as_ref())?;
    }

    store.append_cursor(delta.new_cursor)?;

    // todo: semantics for committing a read transaction

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestEntity {
        value: String,
    }

    impl TestEntity {
        const NS: Namespace = "test";

        pub fn new(value: &str) -> Self {
            Self {
                value: value.to_string(),
            }
        }
    }

    impl Entity for TestEntity {
        fn decode_entity(_: Namespace, value: &EntityValue) -> Result<Self, StateError> {
            let value_str = String::from_utf8(value.clone()).unwrap();
            Ok(TestEntity { value: value_str })
        }

        fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
            (TestEntity::NS, value.value.as_bytes().to_vec())
        }
    }

    struct ChangeValue {
        key: NsKey,
        old_value: Option<String>,
        override_with: String,
    }

    impl EntityDelta for ChangeValue {
        type Entity = TestEntity;

        fn key(&self) -> Cow<'_, NsKey> {
            Cow::Borrowed(&self.key)
        }

        fn apply(&mut self, entity: &mut Option<Self::Entity>) {
            self.old_value = entity.as_ref().map(|e| e.value.clone());

            entity
                .as_mut()
                .map(|e| e.value = self.override_with.clone());
        }

        fn undo(&mut self, entity: &mut Option<Self::Entity>) {
            entity
                .as_mut()
                .map(|e| e.value = self.old_value.clone().unwrap());

            self.old_value = None;
        }
    }

    #[derive(Debug, Clone)]
    struct MockStoreDb {
        cursor: Option<BlockSlot>,
        entities: HashMap<NsKey, EntityValue>,
    }

    #[derive(Debug, Clone)]
    struct MockStore {
        db: Arc<RwLock<MockStoreDb>>,
    }

    impl State3Store for MockStore {
        // type Entity = TestEntity;
        // type EntityDelta = ChangeValue;
        type EntityIter = std::vec::IntoIter<Result<(EntityKey, EntityValue), StateError>>;
        type EntityValueIter = std::iter::Empty<Result<EntityValue, StateError>>;

        fn read_cursor(&self) -> Result<Option<BlockSlot>, StateError> {
            let db = self.db.read().unwrap();
            Ok(db.cursor)
        }

        fn append_cursor(&self, new_cursor: BlockSlot) -> Result<(), StateError> {
            let mut db = self.db.write().unwrap();
            db.cursor = Some(new_cursor);
            Ok(())
        }

        fn read_entities(
            &self,
            ns: Namespace,
            keys: &[&EntityKey],
        ) -> Result<Vec<Option<EntityValue>>, StateError> {
            let db = self.db.read().unwrap();
            let mut out = Vec::with_capacity(keys.len());

            for key in keys {
                let nskey = NsKey(ns, (*key).clone());
                let value = db.entities.get(&nskey).cloned();
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
            let mut db = self.db.write().unwrap();
            let key = NsKey(ns, key.clone());
            db.entities.insert(key, value.clone());
            Ok(())
        }

        fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
            let mut db = self.db.write().unwrap();
            let key = NsKey(ns, key.clone());
            db.entities.remove(&key);
            Ok(())
        }

        fn iter_entities(
            &self,
            ns: Namespace,
            range: Range<EntityKey>,
        ) -> Result<Self::EntityIter, StateError> {
            let db = self.db.read().unwrap();
            let mut out = vec![];

            for nskey in db.entities.keys() {
                if nskey.0 == ns {
                    if range.contains(&nskey.1) {
                        let value = db.entities.get(nskey).unwrap();
                        let pair = (nskey.1.clone(), value.clone());
                        out.push(Ok(pair));
                    }
                }
            }

            Ok(out.into_iter())
        }

        fn iter_entity_values(
            &self,
            ns: Namespace,
            key: impl AsRef<[u8]>,
        ) -> Result<Self::EntityValueIter, StateError> {
            todo!()
        }
    }

    #[derive(Clone)]
    struct MockDomain;

    fn setup_mock_store() -> MockStore {
        let store = MockStore {
            db: Arc::new(RwLock::new(MockStoreDb {
                cursor: Some(0),
                entities: HashMap::new(),
            })),
        };

        store.write_entity_typed(&EntityKey::from(b"a"), &TestEntity::new("123"));

        store.write_entity_typed(&EntityKey::from(b"b"), &TestEntity::new("456"));

        store.write_entity_typed(&EntityKey::from(b"c"), &TestEntity::new("789"));

        store
    }

    #[test]
    fn test_apply_batch() {
        let store = setup_mock_store();

        let mut delta = StateDelta::<ChangeValue>::new(1);

        let delta_a = ChangeValue {
            key: NsKey::from((TestEntity::NS, b"a")),
            override_with: "new_value".into(),
            old_value: None,
        };

        delta.add_delta(delta_a);

        //super::apply_batch::<MockDomain>(&store, &mut delta).unwrap();
    }
}
