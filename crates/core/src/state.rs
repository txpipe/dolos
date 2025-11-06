use std::{collections::HashMap, marker::PhantomData, ops::Range};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{ChainError, ChainPoint, Domain, TxoRef, UtxoMap, UtxoSet, UtxoSetDelta};

pub const KEY_SIZE: usize = 32;

pub type Namespace = &'static str;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EntityKey([u8; KEY_SIZE]);

impl From<&[u8]> for EntityKey {
    fn from(value: &[u8]) -> Self {
        let mut key = [0u8; KEY_SIZE];
        let len = value.len().min(KEY_SIZE);
        key[..len].copy_from_slice(&value[..len]);
        EntityKey(key)
    }
}

impl<const N: usize> From<pallas::crypto::hash::Hash<N>> for EntityKey {
    fn from(value: pallas::crypto::hash::Hash<N>) -> Self {
        EntityKey::from(value.as_slice())
    }
}

impl<const N: usize> From<&[u8; N]> for EntityKey {
    fn from(value: &[u8; N]) -> Self {
        EntityKey::from(value.as_slice())
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

impl EntityKey {
    pub fn full_range() -> Range<EntityKey> {
        Range {
            start: EntityKey([0u8; KEY_SIZE]),
            end: EntityKey([255u8; KEY_SIZE]),
        }
    }
}

impl<const HASH_SIZE: usize> From<EntityKey> for pallas::crypto::hash::Hash<HASH_SIZE> {
    fn from(value: EntityKey) -> Self {
        let mut array = [0u8; HASH_SIZE];
        let source = &value.0[..HASH_SIZE];
        array.copy_from_slice(source);
        pallas::crypto::hash::Hash::<HASH_SIZE>::new(array)
    }
}

/// A namespaced key
///
/// Represent a key to an entity by also specifying the namespace to which it
/// belongs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct NsKey(pub Namespace, pub EntityKey);

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

    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, ChainError>;
    fn encode_entity(value: &Self) -> (Namespace, EntityValue);
}

pub type KeyEntityPair<E> = (EntityKey, Option<E>);

pub trait EntityDelta: Clone + std::fmt::Debug {
    type Entity: Entity;

    fn key(&self) -> NsKey;

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
    fn undo(&self, entity: &mut Option<Self::Entity>);
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

    #[error("entity decoding error")]
    EntityDecodingError(String),
}

pub struct EntityIterTyped<S: StateStore, E: Entity> {
    inner: S::EntityIter,
    ns: Namespace,
    _marker: PhantomData<E>,
}

impl<S: StateStore, E: Entity> EntityIterTyped<S, E> {
    pub fn new(inner: S::EntityIter, ns: Namespace) -> Self {
        Self {
            inner,
            ns,
            _marker: PhantomData,
        }
    }
}

impl<S: StateStore, E: Entity> Iterator for EntityIterTyped<S, E> {
    type Item = Result<(EntityKey, E), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|(key, value)| {
            E::decode_entity(self.ns, &value)
                .map(|v| (key, v))
                .map_err(|x| StateError::EntityDecodingError(x.to_string()))
        });

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

pub trait StateWriter: Sized + Send + Sync {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError>;

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError>;

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError>;

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), StateError>;

    #[allow(clippy::double_must_use)]
    #[must_use]
    fn commit(self) -> Result<(), StateError>;

    fn write_entity_typed<E: Entity>(&self, key: &EntityKey, entity: &E) -> Result<(), StateError> {
        let (ns, raw) = E::encode_entity(entity);

        self.write_entity(ns, key, &raw)
    }

    fn save_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        maybe_entity: Option<&EntityValue>,
    ) -> Result<(), StateError> {
        if let Some(entity) = maybe_entity {
            self.write_entity(ns, key, entity)
        } else {
            self.delete_entity(ns, key)
        }
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
}

pub trait StateStore: Sized + Send + Sync + Clone {
    type EntityIter: Iterator<Item = Result<(EntityKey, EntityValue), StateError>>;
    type EntityValueIter: Iterator<Item = Result<EntityValue, StateError>>;
    type Writer: StateWriter;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError>;

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError>;

    fn start_writer(&self) -> Result<Self::Writer, StateError>;

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
            .map(|x| {
                x.map(|v| {
                    E::decode_entity(ns, &v)
                        .map_err(|x| StateError::EntityDecodingError(x.to_string()))
                })
            })
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

    fn iter_entities_typed<E: Entity>(
        &self,
        ns: Namespace,
        range: Option<Range<EntityKey>>,
    ) -> Result<EntityIterTyped<Self, E>, StateError> {
        let range = range.unwrap_or_else(EntityKey::full_range);

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

    // TODO: generalize UTxO Set into generic entity system

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError>;

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError>;
}

pub fn load_entity_chunk<D: Domain>(
    chunk: &[NsKey],
    store: &D::State,
) -> Result<EntityMap<D::Entity>, StateError> {
    let by_ns = chunk.iter().chunk_by(|key| key.0);

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
