use std::{collections::HashMap, marker::PhantomData};

use itertools::Itertools;

use crate::{
    keys::{EntityKey, KeyCodec, KeyError, KeyRange, RangeOf},
    ChainError, ChainLogic, ChainPoint, Domain, TagDimension, TxoRef, UtxoIndexDelta, UtxoMap,
    UtxoPosition, UtxoSet, UtxoSetDelta, UtxoValue, TAG_ORDER_LEN,
};

pub type Namespace = &'static str;

/// A type whose rows live in one namespace.
pub trait FixedNamespace {
    const NS: Namespace;
}

/// An entity kept in one state namespace, under keys of one kind.
pub trait StateEntity: Entity + FixedNamespace {
    type Key: KeyCodec;
}

/// A namespaced key
///
/// Represent a key to an entity by also specifying the namespace to which it
/// belongs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct NsKey(Namespace, EntityKey);

impl NsKey {
    pub fn of<E: StateEntity>(key: &E::Key) -> Self {
        Self(E::NS, EntityKey::of(key))
    }

    pub fn ns(&self) -> Namespace {
        self.0
    }

    pub fn key(&self) -> &EntityKey {
        &self.1
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

    #[error(transparent)]
    KeyDecodingError(#[from] KeyError),

    /// The operation is part of the trait but the concrete backend does not
    /// implement it.
    ///
    /// Used by backends outside the live set (see `iter_utxos`) so that a
    /// caller reaching for an unimplemented capability gets an error it can
    /// handle instead of a panic.
    #[error("{0} is not supported on this storage backend")]
    Unsupported(&'static str),

    #[error(transparent)]
    IncompatibleStore(#[from] crate::compat::IncompatibleStore),
}

/// A single entry of the UTxO set, as yielded by [`StateStore::iter_utxos`].
///
/// The value is owned, not `Arc`-wrapped: the streaming consumers this exists
/// for (snapshot export, index rebuild) serialize and drop each entry, so
/// shared ownership would be a per-item allocation nobody uses. A caller that
/// wants a [`UtxoMap`] wraps the values itself.
pub type UtxoEntry = (TxoRef, UtxoValue);

/// The direction [`StateStore::utxos_by_tag_page`] walks a tag in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagOrder {
    /// Oldest creation position first.
    Asc,
    /// Newest creation position first.
    Desc,
}

/// A UTxO's entry in a live-UTxO tag: the bytes that order it there
/// ([`UtxoPosition::tag_order`]), then its ref.
///
/// Entries compare in tag order, the order every backend's tag keys sort in:
/// the order bytes, then the ref as its `[tx_hash:32][index:4 BE]` bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TagEntry {
    pub order: [u8; TAG_ORDER_LEN],
    pub txo: TxoRef,
}

impl TagEntry {
    pub fn new(position: Option<UtxoPosition>, txo: TxoRef) -> Self {
        Self {
            order: UtxoPosition::tag_order(position),
            txo,
        }
    }
}

/// Where [`StateStore::utxos_by_tag_page`] starts, in the order it walks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagStart {
    /// Past the first `n` entries.
    Skip(usize),
    /// Just past this entry, which the tag need not hold any more. A walk
    /// that resumes here misses no entry that sits past it, whatever was
    /// written before it.
    After(TagEntry),
}

/// Iterator used by backends that do not implement [`StateStore::iter_utxos`].
///
/// Never constructed — those backends return [`StateError::Unsupported`], so
/// the alias only exists to satisfy the associated type.
pub type EmptyUtxoIter = std::iter::Empty<Result<UtxoEntry, StateError>>;

/// Entities of one namespace with their keys decoded, in key order.
pub struct EntityIterTyped<S: StateStore, E: StateEntity> {
    inner: S::EntityIter,
    _marker: PhantomData<E>,
}

impl<S: StateStore, E: StateEntity> EntityIterTyped<S, E> {
    pub fn new(inner: S::EntityIter) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<S: StateStore, E: StateEntity> Iterator for EntityIterTyped<S, E> {
    type Item = Result<(E::Key, E), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|(key, value)| {
            let key = key.decode::<E::Key>()?;

            E::decode_entity(E::NS, &value)
                .map(|v| (key, v))
                .map_err(|x| StateError::EntityDecodingError(x.to_string()))
        });

        Some(mapped)
    }
}

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

    /// Apply the live-UTxO tag changes that go with a `UtxoSetDelta`.
    ///
    /// The tags are a projection of the UTxO set: `produced` refs gain their
    /// tags, `consumed` refs lose them. They ride the same batch as
    /// `apply_utxoset` and commit with it, so a reader never sees a tag whose
    /// UTxO is not there, or a UTxO without its tags.
    fn apply_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError>;

    /// Reverse `apply_utxo_tags`, its steps undone in reverse: `consumed` refs
    /// regain their tags, then `produced` refs lose them.
    ///
    /// The order decides a ref in both halves, an output the block spent
    /// itself: apply leaves it no tag, so undo must too.
    fn undo_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError>;

    #[allow(clippy::double_must_use)]
    #[must_use]
    fn commit(self) -> Result<(), StateError>;

    fn write_entity_typed<E: StateEntity>(
        &self,
        key: &E::Key,
        entity: &E,
    ) -> Result<(), StateError> {
        let (ns, raw) = E::encode_entity(entity);

        self.write_entity(ns, &EntityKey::of(key), &raw)
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

    fn save_entity_typed<E: StateEntity>(
        &self,
        key: &E::Key,
        maybe_entity: Option<&E>,
    ) -> Result<(), StateError> {
        if let Some(entity) = maybe_entity {
            self.write_entity_typed(key, entity)
        } else {
            self.delete_entity(E::NS, &EntityKey::of(key))
        }
    }
}

pub trait StateStore: Sized + Send + Sync + Clone {
    type EntityIter: Iterator<Item = Result<(EntityKey, EntityValue), StateError>>;
    type EntityValueIter: Iterator<Item = Result<EntityValue, StateError>>;
    type UtxoIter: Iterator<Item = Result<UtxoEntry, StateError>>;
    type Writer: StateWriter;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError>;

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError>;

    fn start_writer(&self) -> Result<Self::Writer, StateError>;

    /// Entities of `ns` whose key falls in `range`, in key order.
    fn iter_entities(&self, ns: Namespace, range: KeyRange)
        -> Result<Self::EntityIter, StateError>;

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError>;

    fn read_entities_typed<E: StateEntity>(
        &self,
        keys: &[&E::Key],
    ) -> Result<Vec<Option<E>>, StateError> {
        let keys: Vec<EntityKey> = keys.iter().map(|key| EntityKey::of(*key)).collect();
        let raw = self.read_entities(E::NS, &keys.iter().collect::<Vec<_>>())?;

        let decoded = raw
            .into_iter()
            .map(|x| {
                x.map(|v| {
                    E::decode_entity(E::NS, &v)
                        .map_err(|x| StateError::EntityDecodingError(x.to_string()))
                })
            })
            .map(|x| x.transpose())
            .try_collect()?;

        Ok(decoded)
    }

    fn read_entity_typed<E: StateEntity>(&self, key: &E::Key) -> Result<Option<E>, StateError> {
        let raw = self.read_entities(E::NS, &[&EntityKey::of(key)])?;

        raw.into_iter()
            .next()
            .unwrap()
            .map(|v| {
                E::decode_entity(E::NS, &v)
                    .map_err(|x| StateError::EntityDecodingError(x.to_string()))
            })
            .transpose()
    }

    fn iter_entities_typed<E: StateEntity>(
        &self,
        range: Option<RangeOf<E::Key>>,
    ) -> Result<EntityIterTyped<Self, E>, StateError> {
        let range = range.unwrap_or_else(RangeOf::all);

        let inner = self.iter_entities(E::NS, range.into_raw())?;

        Ok(EntityIterTyped::<Self, E>::new(inner))
    }

    // fn iter_entity_values_typed<E: Entity>(
    //     &self,
    //     ns: Namespace,
    //     key: impl AsRef<[u8]>,
    // ) -> Result<EntityValueIterTyped<E>, StateError> {
    //     let inner = self.iter_entity_values(ns, key)?;
    //     Ok(EntityValueIterTyped::<E>::new(inner, ns))
    // }

    // TODO: generalize UTxO Set into generic entity system (#1042)

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError>;

    /// Query live UTxOs by tag dimension and key.
    ///
    /// Returns every ref tagged with the given dimension and key that has not
    /// been consumed. Chain-specific code names the dimensions and wraps this
    /// in typed lookups (address, payment credential, asset, …).
    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, StateError>;

    /// One page of the entries of the refs [`StateStore::utxos_by_tag`]
    /// returns, in tag order ([`TagEntry`]), walked in `order` from `start`.
    ///
    /// Returns at most `count` entries. The read stops at `count` past the
    /// start, so a page costs its offset (for [`TagStart::Skip`]) and its
    /// size, not the size of the tag. A backend that keeps the tags of
    /// several lookup keys under one prefix (fjall, where `policy` prefixes
    /// every `policy ‖ name` asset key) also steps over the keys of the longer
    /// lookup keys that sort among them.
    fn utxos_by_tag_page(
        &self,
        dimension: TagDimension,
        key: &[u8],
        order: TagOrder,
        start: TagStart,
        count: usize,
    ) -> Result<Vec<TagEntry>, StateError>;

    /// The entries of `entries` that the tag `(dimension, key)` also holds, in
    /// the order given.
    ///
    /// One point read of each entry's tag key and no UTxO read, so testing an
    /// entry that the tag does not hold costs what testing one it holds does.
    fn utxos_with_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        entries: Vec<TagEntry>,
    ) -> Result<Vec<TagEntry>, StateError>;

    /// Iterate the whole UTxO set.
    ///
    /// The iterator must be lazy: neither construction nor iteration may
    /// buffer the set, since callers (snapshot export, live-UTxO index
    /// rebuild) run against a mainnet-sized set that does not fit a memory
    /// budget. Construction reads nothing; the first read failure surfaces
    /// from `next()`.
    ///
    /// **Errors are terminal.** A malformed on-disk entry or a read failure is
    /// yielded as `Err` and the iterator is fused from then on: it never
    /// resumes past a fault, so a consumer cannot receive a silently truncated
    /// UTxO set.
    ///
    /// Backends that do not implement this return
    /// [`StateError::Unsupported`].
    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError>;
}

pub fn load_entity_chunk<D: Domain>(
    chunk: &[NsKey],
    store: &D::State,
) -> Result<EntityMap<D::Entity>, StateError> {
    let by_ns = chunk.iter().chunk_by(|key| key.ns());

    let mut loaded: EntityMap<D::Entity> = HashMap::new();

    for (ns, chunk) in &by_ns {
        let chunk = chunk.collect::<Vec<_>>();
        let keys = chunk.iter().map(|x| x.key()).collect::<Vec<_>>();

        let raw = store.read_entities(ns, &keys)?;

        for (key, value) in chunk.into_iter().zip(raw) {
            let value = value
                .map(|v| {
                    D::Entity::decode_entity(ns, &v)
                        .map_err(|x| StateError::EntityDecodingError(x.to_string()))
                })
                .transpose()?;

            loaded.insert(key.clone(), value);
        }
    }

    Ok(loaded)
}

/// Load into `entities` any entity referenced by `deltas` that isn't already
/// tracked. Entities already in the map keep their in-memory value, so the
/// map doubles as a read-your-own-writes cache for callers that replay
/// multiple delta chunks before committing.
fn load_missing_entities<D: Domain>(
    entities: &mut EntityMap<D::Entity>,
    store: &D::State,
    deltas: &[D::EntityDelta],
) -> Result<(), StateError> {
    let missing: Vec<NsKey> = deltas
        .iter()
        .map(|delta| delta.key())
        .filter(|key| !entities.contains_key(key))
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    let loaded = load_entity_chunk::<D>(missing.as_slice(), store)?;
    entities.extend(loaded);

    Ok(())
}

/// Replay a chunk of deltas forward over the tracked entities, loading any
/// entity not already in the map from the store.
///
/// This is the same delta application that runs in-memory during normal
/// sync before `commit_state` persists the results; it's shared here so
/// crash-recovery WAL replay (bootstrap catch-up) uses the exact same logic.
pub fn apply_delta_chunk<D: Domain>(
    entities: &mut EntityMap<D::Entity>,
    store: &D::State,
    deltas: &mut [D::EntityDelta],
) -> Result<(), StateError> {
    load_missing_entities::<D>(entities, store, deltas)?;

    for delta in deltas.iter_mut() {
        let entity = entities
            .get_mut(&delta.key())
            .expect("entity loaded by load_missing_entities");

        delta.apply(entity);
    }

    Ok(())
}

/// Counterpart of [`apply_delta_chunk`]: undo a chunk of deltas over the
/// tracked entities.
///
/// Deltas are undone in reverse application order. Each delta's `prev_*`
/// captures the state immediately before its own apply, so multiple deltas
/// keyed to the same entity must be reversed last-first to correctly walk
/// back through the apply chain.
///
/// A delta that finds its entity absent applied before boundary work deleted
/// it. The chain may restore the entity for the undo
/// ([`ChainLogic::restore_for_undo`]), and its key joins `restored`.
pub fn undo_delta_chunk<D: Domain>(
    entities: &mut EntityMap<D::Entity>,
    restored: &mut Vec<NsKey>,
    store: &D::State,
    deltas: &[D::EntityDelta],
) -> Result<(), StateError> {
    load_missing_entities::<D>(entities, store, deltas)?;

    for delta in deltas.iter().rev() {
        let key = delta.key();

        let entity = entities
            .get_mut(&key)
            .expect("entity loaded by load_missing_entities");

        if entity.is_none() {
            *entity = D::Chain::restore_for_undo(store, &key)?;

            if entity.is_some() && !restored.contains(&key) {
                restored.push(key);
            }
        }

        delta.undo(entity);
    }

    Ok(())
}

/// Persist one entity at `key` through the writer: `Some` upserts the record,
/// `None` deletes it.
pub fn save_entity_at<E: Entity>(
    writer: &impl StateWriter,
    key: &NsKey,
    entity: Option<&E>,
) -> Result<(), StateError> {
    let raw = entity.map(E::encode_entity);
    writer.save_entity(key.ns(), key.key(), raw.as_ref().map(|(_, raw)| raw))
}

/// Persist every tracked entity through the writer: `Some` upserts the
/// record, `None` deletes it.
pub fn save_entities<D: Domain>(
    writer: &<D::State as StateStore>::Writer,
    entities: &EntityMap<D::Entity>,
) -> Result<(), StateError> {
    for (key, entity) in entities.iter() {
        save_entity_at(writer, key, entity.as_ref())?;
    }

    Ok(())
}
