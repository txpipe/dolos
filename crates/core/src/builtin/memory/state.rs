//! In-memory state store backed by ordered maps.
//!
//! The disk backends spend most of their code encoding composite keys into a
//! flat, lexicographically ordered keyspace. A [`BTreeMap`] keyed on the native
//! tuple is already that keyspace, so this implementation is the store contract
//! with the encoding removed:
//!
//! - entities live in one map keyed `(namespace, entity_key)`, which makes a
//!   namespaced range scan a plain [`BTreeMap::range`] — the same shape fjall
//!   gets from its `[ns_hash:8][entity_key:32]` prefix;
//! - UTxOs live in a map keyed on [`TxoRef`], whose `(tx_hash, index)` ordering
//!   is exactly what fjall's `[tx_hash:32][index:4]` big-endian key encodes;
//! - the cursor is one slot.
//!
//! ## Ephemeral by design
//!
//! Nothing here persists and nothing here is sized for a mainnet-shaped UTxO
//! set: the iterators materialize their range into an owned buffer before
//! yielding, so a full traversal transiently doubles the memory the set already
//! occupies. That is the trade this backend makes — it exists for devnets,
//! tooling and tests, where it is the fastest store available and the only
//! memory-resident one that serves the snapshot export seam.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};

use crate::state::{
    EntityKey, EntityValue, Namespace, StateError, StateStore, StateWriter, UtxoEntry,
};
use crate::{ChainPoint, EraCbor, TxoRef, UtxoMap, UtxoSetDelta};

/// The store's whole contents, guarded as one unit so a commit is atomic.
#[derive(Default)]
struct Tables {
    cursor: Option<ChainPoint>,
    entities: BTreeMap<(Namespace, EntityKey), EntityValue>,
    /// Values stay behind the `Arc` they arrive in, so `get_utxos` — the
    /// hottest read on this store — hands them out without copying a CBOR body
    /// per lookup.
    utxos: BTreeMap<TxoRef, Arc<EraCbor>>,
}

/// A single mutation, recorded by a writer and replayed at commit.
///
/// Keeping the writer's work as an ordered log rather than a merged map is what
/// makes the commit agree with a disk write batch: within one batch, a later
/// operation on a key supersedes an earlier one, so a UTxO both produced and
/// consumed by the same delta ends up absent rather than present.
enum Op {
    SetCursor(ChainPoint),
    WriteEntity(Namespace, EntityKey, EntityValue),
    DeleteEntity(Namespace, EntityKey),
    PutUtxo(TxoRef, Arc<EraCbor>),
    DeleteUtxo(TxoRef),
}

fn poisoned() -> StateError {
    StateError::InternalStoreError("state store lock poisoned".into())
}

/// State store held entirely in memory.
///
/// Cloning shares the underlying tables, which is what [`StateStore`]'s `Clone`
/// bound means everywhere else: a clone is another handle on the same store,
/// not a copy of it.
#[derive(Clone, Default)]
pub struct MemoryStateStore {
    tables: Arc<RwLock<Tables>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Present for symmetry with the disk backends, which need it to drain
    /// background work before the process exits. There is nothing to drain
    /// here.
    pub fn shutdown(&self) -> Result<(), StateError> {
        Ok(())
    }
}

/// Writer that accumulates its mutations privately and merges them under a
/// single write lock at [`StateWriter::commit`].
///
/// The lock is not held for the writer's lifetime: readers keep running while a
/// writer is open, and see nothing of its work until the commit lands — the
/// same visibility a disk transaction gives.
pub struct MemoryStateWriter {
    store: MemoryStateStore,
    ops: Mutex<Vec<Op>>,
}

impl MemoryStateWriter {
    fn push(&self, op: Op) -> Result<(), StateError> {
        self.ops.lock().map_err(|_| poisoned())?.push(op);
        Ok(())
    }
}

impl StateWriter for MemoryStateWriter {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        self.push(Op::SetCursor(cursor))
    }

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError> {
        self.push(Op::WriteEntity(ns, key.clone(), value.clone()))
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        self.push(Op::DeleteEntity(ns, key.clone()))
    }

    /// Ordered as the disk backends order it — produced, recovered, consumed,
    /// undone — because the order decides the outcome when a delta touches the
    /// same ref twice.
    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), StateError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for (txo, era_cbor) in &delta.produced_utxo {
            ops.push(Op::PutUtxo(txo.clone(), era_cbor.clone()));
        }

        for (txo, era_cbor) in &delta.recovered_stxi {
            ops.push(Op::PutUtxo(txo.clone(), era_cbor.clone()));
        }

        for txo in delta.consumed_utxo.keys() {
            ops.push(Op::DeleteUtxo(txo.clone()));
        }

        for txo in delta.undone_utxo.keys() {
            ops.push(Op::DeleteUtxo(txo.clone()));
        }

        Ok(())
    }

    fn commit(self) -> Result<(), StateError> {
        let ops = self.ops.into_inner().map_err(|_| poisoned())?;

        let mut tables = self.store.tables.write().map_err(|_| poisoned())?;

        for op in ops {
            match op {
                Op::SetCursor(cursor) => tables.cursor = Some(cursor),
                Op::WriteEntity(ns, key, value) => {
                    tables.entities.insert((ns, key), value);
                }
                Op::DeleteEntity(ns, key) => {
                    tables.entities.remove(&(ns, key));
                }
                Op::PutUtxo(txo, value) => {
                    tables.utxos.insert(txo, value);
                }
                Op::DeleteUtxo(txo) => {
                    tables.utxos.remove(&txo);
                }
            }
        }

        Ok(())
    }
}

/// Entity iterator over a materialized range.
pub struct MemoryEntityIter(std::vec::IntoIter<(EntityKey, EntityValue)>);

impl Iterator for MemoryEntityIter {
    type Item = Result<(EntityKey, EntityValue), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

/// UTxO iterator over a materialized snapshot of the set.
///
/// Unlike the disk backends this is not lazy — see the module docs. It is still
/// a point-in-time view: the buffer is filled under a read lock, so a
/// concurrent writer cannot tear it.
pub struct MemoryUtxoIter(std::vec::IntoIter<UtxoEntry>);

impl Iterator for MemoryUtxoIter {
    type Item = Result<UtxoEntry, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

impl StateStore for MemoryStateStore {
    type EntityIter = MemoryEntityIter;
    type EntityValueIter = std::iter::Empty<Result<EntityValue, StateError>>;
    type UtxoIter = MemoryUtxoIter;
    type Writer = MemoryStateWriter;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables.cursor.clone())
    }

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        let values = keys
            .iter()
            .map(|key| tables.entities.get(&(ns, (*key).clone())).cloned())
            .collect();

        Ok(values)
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        Ok(MemoryStateWriter {
            store: self.clone(),
            ops: Mutex::new(Vec::new()),
        })
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        // An inverted range matches nothing on the disk backends, whose keys
        // are bytes; `BTreeMap::range` panics on one, so it is caught here
        // rather than turned into a difference between backends.
        if range.start > range.end {
            return Ok(MemoryEntityIter(Vec::new().into_iter()));
        }

        let tables = self.tables.read().map_err(|_| poisoned())?;

        let entries: Vec<_> = tables
            .entities
            .range((ns, range.start)..(ns, range.end))
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect();

        Ok(MemoryEntityIter(entries.into_iter()))
    }

    /// Multi-value namespaces have no definition this backend could honor: the
    /// live persistent backend does not implement them either, so there is no
    /// behavior to conform to. Refusing is the documented answer for a
    /// capability a backend does not carry.
    fn iter_entity_values(
        &self,
        _ns: Namespace,
        _key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        Err(StateError::Unsupported("iter_entity_values"))
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        let found = refs
            .into_iter()
            .filter_map(|txo| tables.utxos.get(&txo).map(|value| (txo, value.clone())))
            .collect();

        Ok(found)
    }

    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        let entries: Vec<UtxoEntry> = tables
            .utxos
            .iter()
            .map(|(txo, value)| (txo.clone(), value.as_ref().clone()))
            .collect();

        Ok(MemoryUtxoIter(entries.into_iter()))
    }
}
