//! Reading a stele back into an empty store set.
//!
//! [`crate::export`]'s inverse, and the half that closes the loop: a node with
//! no data rebuilds itself from a directory of layers and is indistinguishable
//! from one built by replaying the chain.
//!
//! ## The order is the specification
//!
//! ADR-004 §"Restore pipeline" fixes the sequence, and it is not an
//! implementation preference — each step exists because of what the one before
//! it established:
//!
//! 1. Read the inscription, check the profile, and refuse a stele whose
//!    `position.network.magic` is not this node's — **before anything is
//!    written**, because half a mainnet ledger under a preprod configuration is
//!    not a state a node recovers from.
//! 2. Select layers ([`plan`]) and preflight free space ([`Plan::preflight`]).
//!    Selection is profile-side by necessity: a layer's `scope` is opaque to
//!    the protocol, so nothing but this crate can read an epoch out of one.
//! 3. [`dolos_core::IndexStore::initialize_schema`].
//! 4. Per epoch: `blocks`, then `logs`, then `indexes`.
//! 5. The state tip, sixteen shards, `set_cursor` **last**.
//! 6. Rebuild the live-UTxO index dimensions from the restored UTxO set. They
//!    are never shipped — ADR-004's Amendment 2 — so this is where they come
//!    back.
//!
//! Nothing is added for the WAL: `bootstrap::run` already reseeds it from the
//! state cursor after any bootstrap method.
//!
//! ## Why `set_cursor` is last, and what that does and does not buy
//!
//! `has_existing_data()` reads the state cursor and nothing else, so writing it
//! only after every shard has landed means an interrupted restore leaves a
//! store set the next `bootstrap` treats as empty rather than as a node.
//!
//! It buys that and no more. Step 6 runs *after* the cursor is set, so an
//! interruption between the two leaves a node whose ledger is complete and
//! whose live-UTxO indexes are not — and `has_existing_data()` will say it is
//! restored. Closing that window is what ADR-004's progress file is for, and
//! resumability is verified in Phase 3; until then a restore that fails must be
//! re-run with `--force`.
//!
//! ## Memory
//!
//! Nothing here holds a layer. Every layer is read through
//! [`stelae::dir::SteleDir::stream_layer`] under [`Limits`] and drained into
//! bounded chunks — a mainnet state shard is 402 MB and one epoch of blocks
//! runs past a gigabyte, so [`stelae::dir::SteleDir::read_layer`] is never on
//! this path.
//!
//! ## A layer is only proven by `finish`
//!
//! A layer's `diffId` covers its whole byte string, so it cannot be confirmed
//! until the last record has gone past; [`stelae::LayerReader`] states the
//! consequence plainly, that records are consumable before the layer is proven.
//! [`drain`] therefore withholds the final chunk until `finish` returns `Ok`,
//! and any earlier chunk that was already committed is harmless for the same
//! reason `set_cursor` is last: a restore that fails leaves no cursor, so what
//! it wrote is not a node.

use std::{collections::BTreeMap, path::Path, sync::Arc};

use dolos_cardano::indexes::index_delta_from_utxo_delta;
use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, ChainPoint, EraCbor, IndexRecord, IndexStore,
    IndexWriter, StateStore, StateWriter, TxoRef, UtxoSetDelta,
};
use stelae::{
    dir::{BlobIndex, SteleDir},
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    LayerHeader,
};
use tracing::info;

use crate::{
    layers::{blocks, indexes, logs, state},
    read_position, DolosProfile, Error, Position, BLOCKS, DIGESTS, INDEXES, LOGS, STATE,
    STATE_SHARDS, UTXOS,
};

/// What a restore holds at once.
///
/// A store writer batches until `commit` and a layer arrives as a stream, so
/// nothing bounds a restore's memory except these numbers. Both commit ceilings
/// are needed and neither subsumes the other: an index record is tens of bytes
/// and only a count bounds it, while one epoch of mainnet blocks is gigabytes
/// and only a byte budget bounds that.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    /// Per-record and window bounds on the layer read itself.
    pub limits: Limits,
    /// Records accumulated before a write batch is committed.
    pub commit_records: usize,
    /// Bytes accumulated before a write batch is committed.
    pub commit_bytes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            commit_records: 50_000,
            commit_bytes: 64 * 1024 * 1024,
        }
    }
}

/// The per-epoch layers a restore consumes, for one epoch.
///
/// Every kind is optional: ADR-004 makes layers individually non-mandatory, so
/// a ledger-only stele carrying no `blocks` is valid and restores into a node
/// with no chain history rather than into a refusal.
#[derive(Debug, Clone)]
pub struct EpochLayers {
    pub epoch: u64,
    pub start_slot: BlockSlot,
    pub end_slot: BlockSlot,
    pub blocks: Option<LayerDescriptor>,
    pub logs: Option<LayerDescriptor>,
    pub indexes: Option<LayerDescriptor>,
}

impl EpochLayers {
    fn descriptors(&self) -> impl Iterator<Item = &LayerDescriptor> {
        [&self.blocks, &self.logs, &self.indexes]
            .into_iter()
            .flatten()
    }
}

/// What a restore will consume, decided before a single byte is written.
///
/// The mirror of [`crate::export::Plan`]: built up front so a report of what
/// *would* happen and the restore itself agree by construction.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Where the stele stands. Its magic has already been checked against the
    /// node's — [`plan`] refuses a mismatch rather than recording one.
    pub position: Position,
    /// The stele's `sequence`, which for this profile is the epoch its cursor
    /// has just entered.
    pub sequence: u64,
    /// The epochs whose layers this restore consumes, ascending.
    pub epochs: Vec<EpochLayers>,
    /// The sixteen state shards, ascending.
    pub state: Vec<LayerDescriptor>,
    /// Epochs the stele carries and `sync.max_history` excludes.
    pub skipped_epochs: usize,
}

impl Plan {
    /// Every layer this restore will read.
    pub fn layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.epochs
            .iter()
            .flat_map(EpochLayers::descriptors)
            .chain(self.state.iter())
    }

    /// Uncompressed bytes across the selected layers.
    ///
    /// Not [`Inscription::uncompressed_size`], which sums the whole document:
    /// what a restore needs room for is what it is going to read, and an epoch
    /// dropped by `sync.max_history` is never fetched.
    pub fn uncompressed_size(&self) -> u64 {
        self.layers().map(|l| l.uncompressed_size).sum()
    }

    /// Refuse a restore that cannot fit, before it starts writing.
    ///
    /// The comparison is deliberately against the *uncompressed* size of the
    /// selected layers rather than against a prediction of what the stores will
    /// occupy. It is the only number the inscription carries, it is an
    /// underestimate for every backend (a store keeps indexes and slack of its
    /// own), and an underestimate is the safe direction for a check whose job
    /// is to catch the obviously-doomed run.
    pub fn preflight(&self, path: &Path) -> Result<(), Error> {
        let required = self.uncompressed_size();

        // The storage root may not exist yet on a fresh node, in which case the
        // filesystem to ask about is the nearest ancestor that does.
        let mut probe = path;

        let available = loop {
            match fs4::available_space(probe) {
                Ok(available) => break available,
                Err(e) => match probe.parent() {
                    Some(parent) => probe = parent,
                    // Nothing left to ask. A restore that cannot measure the
                    // disk is not a restore that should refuse to run, but it
                    // is one whose operator should hear about it.
                    None => {
                        tracing::warn!(
                            path = %path.display(),
                            "could not determine free space; skipping the restore preflight: {e}"
                        );
                        return Ok(());
                    }
                },
            }
        };

        if available < required {
            return Err(Error::IncompleteStele(format!(
                "restoring it needs at least {required} bytes at {}, which has {available} free",
                path.display(),
            )));
        }

        Ok(())
    }
}

/// Read a stele's inscription and decide what restoring it into this node
/// means.
///
/// `network_magic` is the node's own, from its genesis; a stele that disagrees
/// is refused here, which is the whole reason this function exists separately
/// from [`restore`]. `max_history` is `sync.max_history` in slots: epochs whose
/// layers fall entirely below `cursor - max_history` are dropped, which is what
/// ADR-004 replaced the old `full`/`ledger` snapshot variants with.
pub fn plan(stele: &SteleDir, network_magic: u64, max_history: Option<u64>) -> Result<Plan, Error> {
    let inscription = stele.read_inscription()?;

    // Refuses a foreign profile, a profile major above this one, and any layer
    // kind this profile does not define — all before a store is opened.
    inscription.check_profile(&DolosProfile)?;

    let position = read_position(&inscription.position)?;

    if position.network.magic() != network_magic {
        return Err(Error::NetworkMismatch {
            expected: network_magic,
            found: position.network.magic(),
        });
    }

    let epochs = select_epochs(&inscription)?;
    let selected = retain_history(epochs, position.point.slot(), max_history);

    Ok(Plan {
        sequence: inscription.sequence,
        state: select_state(&inscription)?,
        epochs: selected.0,
        skipped_epochs: selected.1,
        position,
    })
}

/// Group the epoch-scoped layers by the epoch their scope names.
fn select_epochs(inscription: &Inscription) -> Result<Vec<EpochLayers>, Error> {
    let mut by_epoch: BTreeMap<u64, EpochLayers> = BTreeMap::new();

    for descriptor in &inscription.layers {
        let kind = descriptor.kind.as_str();

        if !matches!(kind, BLOCKS | LOGS | INDEXES) {
            continue;
        }

        let epoch = scope_uint(descriptor, "epoch")?;
        let start_slot = scope_uint(descriptor, "startSlot")?;
        let end_slot = scope_uint(descriptor, "endSlot")?;

        let entry = by_epoch.entry(epoch).or_insert_with(|| EpochLayers {
            epoch,
            start_slot,
            end_slot,
            blocks: None,
            logs: None,
            indexes: None,
        });

        // One epoch, one window: three kinds describing the same epoch with
        // different bounds is a stele nobody can reason about, and picking one
        // of them would be this crate inventing an answer.
        if (entry.start_slot, entry.end_slot) != (start_slot, end_slot) {
            return Err(Error::malformed_inscription(
                format!("layers[{kind}].scope"),
                format!(
                    "epoch {epoch} is {}..={} in one layer and {start_slot}..={end_slot} in another",
                    entry.start_slot, entry.end_slot,
                ),
            ));
        }

        let slot = match kind {
            BLOCKS => &mut entry.blocks,
            LOGS => &mut entry.logs,
            _ => &mut entry.indexes,
        };

        if slot.is_some() {
            return Err(Error::malformed_inscription(
                format!("layers[{kind}].scope"),
                format!("epoch {epoch} is described twice"),
            ));
        }

        *slot = Some(descriptor.clone());
    }

    Ok(by_epoch.into_values().collect())
}

/// The sixteen state shards, ascending.
///
/// Every shard must be there, including an empty one. A missing shard is a
/// missing slice of the ledger that no later step would notice — the write path
/// dispatches on the namespace, not the shard — so it is refused rather than
/// restored into a node whose queries quietly miss a sixteenth of the state.
fn select_state(inscription: &Inscription) -> Result<Vec<LayerDescriptor>, Error> {
    let mut by_shard: BTreeMap<u64, LayerDescriptor> = BTreeMap::new();

    for descriptor in inscription.layers_of_kind(STATE) {
        let shard = scope_uint(descriptor, "shard")?;

        if by_shard.insert(shard, descriptor.clone()).is_some() {
            return Err(Error::malformed_inscription(
                "layers[state].scope",
                format!("shard {shard} is described twice"),
            ));
        }
    }

    let expected: Vec<u64> = (0..STATE_SHARDS).collect();
    let found: Vec<u64> = by_shard.keys().copied().collect();

    if found != expected {
        return Err(Error::IncompleteStele(format!(
            "the state tip needs all {STATE_SHARDS} shards, and this stele carries {found:?}",
        )));
    }

    Ok(by_shard.into_values().collect())
}

/// Drop the epochs `sync.max_history` puts out of reach.
///
/// `max_history` is a slot window measured back from the tip — the same unit
/// the node prunes its archive by — so an epoch survives if any of its slots
/// falls inside it.
fn retain_history(
    epochs: Vec<EpochLayers>,
    tip: BlockSlot,
    max_history: Option<u64>,
) -> (Vec<EpochLayers>, usize) {
    let Some(max_history) = max_history else {
        return (epochs, 0);
    };

    let floor = tip.saturating_sub(max_history);
    let total = epochs.len();

    let kept: Vec<EpochLayers> = epochs
        .into_iter()
        .filter(|window| window.end_slot >= floor)
        .collect();

    let skipped = total - kept.len();

    (kept, skipped)
}

/// A `u64` field of a layer's profile-owned scope.
fn scope_uint(descriptor: &LayerDescriptor, field: &str) -> Result<u64, Error> {
    descriptor
        .scope
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            Error::malformed_inscription(
                format!("layers[{}].scope.{field}", descriptor.kind),
                format!("missing or not a u64 in {}", descriptor.scope),
            )
        })
}

/// What a restore wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    pub blocks: u64,
    pub logs: u64,
    pub index_records: u64,
    pub entities: u64,
    pub utxos: u64,
}

/// Restore `plan`'s layers into an empty store set.
///
/// The stores are the caller's and are expected to be empty: this writes, it
/// never clears. Emptiness is `bootstrap`'s business, and it already owns
/// `--force`.
pub fn restore<A, S, I>(
    stele: &SteleDir,
    index: &BlobIndex,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    budget: Budget,
) -> Result<Summary, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let reader = Reader {
        stele,
        index,
        magic: plan.position.network.magic(),
        budget,
    };

    let mut summary = Summary::default();

    indexes.initialize_schema()?;

    for epoch in &plan.epochs {
        info!(
            epoch = epoch.epoch,
            slots = format!("{}..={}", epoch.start_slot, epoch.end_slot),
            "restoring epoch"
        );

        if let Some(descriptor) = &epoch.blocks {
            summary.blocks += restore_blocks(&reader, descriptor, archive)?;
        }

        if let Some(descriptor) = &epoch.logs {
            summary.logs += restore_logs(&reader, descriptor, archive)?;
        }

        if let Some(descriptor) = &epoch.indexes {
            summary.index_records += restore_indexes(&reader, descriptor, indexes)?;
        }
    }

    info!(shards = plan.state.len(), "restoring the state tip");

    for descriptor in &plan.state {
        let (entities, utxos) = restore_state(&reader, descriptor, state)?;

        summary.entities += entities;
        summary.utxos += utxos;
    }

    // Last, so that until this commit lands `has_existing_data()` reports an
    // empty node rather than a half-restored one.
    let writer = state.start_writer()?;
    writer.set_cursor(plan.position.point.clone())?;
    writer.commit()?;

    info!(utxos = summary.utxos, "rebuilding the live-utxo indexes");

    rebuild_utxo_indexes(state, indexes, &plan.position.point, budget)?;

    Ok(summary)
}

/// Open, verify and read `root` into the stores, in one call.
///
/// The front door for a caller holding a path and a configuration — the
/// bootstrap command above all — so the profile stays the only thing in the
/// binary that names the protocol crate.
///
/// `blob_index` is the expensive part and is unavoidable for a directory: an
/// inscription names layers by identity and a directory has no manifest, so the
/// map from a descriptor to the file holding it is rebuilt by decompressing
/// every blob once. A registry supplies it for free, which is Phase 3.
pub fn restore_dir<A, S, I>(
    root: impl Into<std::path::PathBuf>,
    network_magic: u64,
    max_history: Option<u64>,
    storage_path: &Path,
    archive: &A,
    state: &S,
    indexes: &I,
) -> Result<(Plan, Summary), Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let stele = SteleDir::open(root)?;

    let plan = plan(&stele, network_magic, max_history)?;
    plan.preflight(storage_path)?;

    let index = stele.blob_index()?;
    let summary = restore(
        &stele,
        &index,
        &plan,
        archive,
        state,
        indexes,
        Budget::default(),
    )?;

    Ok((plan, summary))
}

/// The stele a restore is reading, and the terms it reads under.
///
/// Carried as one value because every layer is read the same way, and because
/// the alternative is threading four unchanging arguments through each of the
/// four per-kind drivers.
struct Reader<'a> {
    stele: &'a SteleDir,
    index: &'a BlobIndex,
    /// The magic every layer's header has to name. Taken from the stele's
    /// `position`, which [`plan`] has already held against the node's — so
    /// checking a layer against it checks the layer against the node.
    magic: u64,
    budget: Budget,
}

impl Reader<'_> {
    /// Stream one layer's records into `flush`, one bounded chunk at a time.
    ///
    /// The single read path for every kind, so the verification discipline is
    /// stated once: the header's network magic is checked against the node's
    /// before a record is handed out, and the layer's own `finish` — which is
    /// what proves its identity digest, its size and its record count — runs
    /// *before* the final chunk is written.
    fn drain<T>(
        &self,
        descriptor: &LayerDescriptor,
        decode: fn(&[u8]) -> Result<T, Error>,
        size: impl Fn(&T) -> usize,
        mut flush: impl FnMut(Vec<T>) -> Result<(), Error>,
    ) -> Result<u64, Error> {
        let mut layer =
            self.stele
                .stream_layer(self.index, &DolosProfile, descriptor, self.budget.limits)?;

        check_layer_magic(descriptor, layer.header(), self.magic)?;

        let mut count = 0u64;
        let mut chunk: Vec<T> = Vec::new();
        let mut bytes = 0usize;

        while let Some(record) = layer.next_record() {
            let record = decode(record?)?;

            count += 1;
            bytes += size(&record);
            chunk.push(record);

            if chunk.len() >= self.budget.commit_records || bytes >= self.budget.commit_bytes {
                flush(std::mem::take(&mut chunk))?;
                bytes = 0;
            }
        }

        layer.finish()?;

        if !chunk.is_empty() {
            flush(chunk)?;
        }

        Ok(count)
    }
}

/// A layer whose header names another chain, refused before its records reach a
/// store.
///
/// The inscription's `position` is checked once, in [`plan`]; this is the same
/// question asked of every blob, because the magic is the only field of a
/// layer's scope that is *not* in the descriptor — it rides in the header
/// record alone. The `diffId` already binds that header to the signed document,
/// so this catches a publisher mistake rather than an attack, and it catches it
/// at the layer that made it.
fn check_layer_magic(
    descriptor: &LayerDescriptor,
    header: &LayerHeader,
    expected: u64,
) -> Result<(), Error> {
    let bytes = header.scope.as_bytes();
    let mut decoder = minicbor::Decoder::new(bytes);

    let malformed = |reason: &str| {
        Error::malformed_inscription(format!("layers[{}] header scope", descriptor.kind), reason)
    };

    decoder
        .array()
        .map_err(|e| malformed(&format!("expected an array: {e}")))?
        .ok_or_else(|| malformed("indefinite-length array"))?;

    let found = decoder
        .u64()
        .map_err(|e| malformed(&format!("network_magic: {e}")))?;

    if found != expected {
        return Err(Error::NetworkMismatch { expected, found });
    }

    Ok(())
}

/// One epoch's blocks, appended to the archive in stream order.
fn restore_blocks<A: ArchiveStore>(
    reader: &Reader<'_>,
    descriptor: &LayerDescriptor,
    archive: &A,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        blocks::decode,
        |record| record.body.len(),
        |chunk| {
            let writer = archive.start_writer()?;

            for record in chunk {
                let point = ChainPoint::Specific(record.slot, record.hash);

                writer.apply(&point, &Arc::new(record.body))?;
            }

            writer.commit()?;

            Ok(())
        },
    )
}

/// One epoch's ledger logs.
fn restore_logs<A: ArchiveStore>(
    reader: &Reader<'_>,
    descriptor: &LayerDescriptor,
    archive: &A,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        logs::decode,
        |record| record.value.len() + logs::LOG_KEY_LEN,
        |chunk| {
            let writer = archive.start_writer()?;

            for record in chunk {
                writer.write_log(record.ns, &record.key, &record.value)?;
            }

            writer.commit()?;

            Ok(())
        },
    )
}

/// One epoch's index records, appended in the stored key form they travel in.
///
/// `append_prehashed` takes the records as they came off the wire: there is no
/// logical key to recover, and recomputing one is not merely wasteful but
/// impossible for the dimension whose stored form is a verbatim label. Chunking
/// is this caller's, which is what the trait says, and the sort order the
/// backends want holds across the whole layer because that is what the codec's
/// `OrderCheck` made the exporter prove.
fn restore_indexes<I: IndexStore>(
    reader: &Reader<'_>,
    descriptor: &LayerDescriptor,
    indexes: &I,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        indexes::decode,
        |_| std::mem::size_of::<IndexRecord>(),
        |chunk| {
            let writer = indexes.start_writer()?;

            writer.append_prehashed(chunk)?;
            writer.commit()?;

            Ok(())
        },
    )
}

/// One state shard, dispatched per namespace.
///
/// Returns the entities and the UTxOs it wrote, separately, because they are
/// the two halves the cross-check compares and a shard that restored one and
/// silently dropped the other would still add up.
fn restore_state<S: StateStore>(
    reader: &Reader<'_>,
    descriptor: &LayerDescriptor,
    state: &S,
) -> Result<(u64, u64), Error> {
    let mut entities = 0u64;
    let mut utxos = 0u64;

    reader.drain(
        descriptor,
        state::decode,
        |record| record.key.len() + record.value.len(),
        |chunk| {
            let writer = state.start_writer()?;

            // The UTxO set has its own writer method rather than a per-record
            // one, so it is collected into a delta and applied once per chunk.
            let mut produced = UtxoSetDelta::default();

            for record in chunk {
                if record.ns == UTXOS {
                    let (txo, value) = state::as_utxo(&record)?;

                    produced.produced_utxo.insert(txo, Arc::new(value));
                    utxos += 1;
                } else {
                    let (ns, key) = state::as_entity(&record)?;

                    writer.write_entity(ns, &key, &record.value)?;
                    entities += 1;
                }
            }

            if !produced.produced_utxo.is_empty() {
                writer.apply_utxoset(&produced)?;
            }

            writer.commit()?;

            Ok(())
        },
    )?;

    Ok((entities, utxos))
}

/// Rebuild the live-UTxO index dimensions from the restored UTxO set.
///
/// `utxo::{address,payment,stake,policy,asset}` track the current UTxO set, so
/// ADR-004's Amendment 2 leaves them out of the epoch layers and rebuilds them
/// here: linear over a set that has just been written anyway, and cheaper than
/// shipping them.
///
/// The last call also aligns the index cursor, which
/// [`IndexWriter::append_prehashed`] deliberately never touches. It runs
/// unconditionally — a stele with an empty UTxO set still has to leave a cursor
/// behind, or `bootstrap` reads the index store as never indexed.
fn rebuild_utxo_indexes<S: StateStore, I: IndexStore>(
    state: &S,
    indexes: &I,
    cursor: &ChainPoint,
    budget: Budget,
) -> Result<(), Error> {
    let mut chunk: Vec<(TxoRef, Arc<EraCbor>)> = Vec::new();

    let apply = |chunk: Vec<(TxoRef, Arc<EraCbor>)>| -> Result<(), Error> {
        let delta = UtxoSetDelta {
            produced_utxo: chunk.into_iter().collect(),
            ..Default::default()
        };

        let writer = indexes.start_writer()?;

        writer.apply(&index_delta_from_utxo_delta(cursor.clone(), &delta))?;
        writer.commit()?;

        Ok(())
    };

    for entry in state.iter_utxos()? {
        let (txo, value) = entry?;

        chunk.push((txo, Arc::new(value)));

        if chunk.len() >= budget.commit_records {
            apply(std::mem::take(&mut chunk))?;
        }
    }

    // Unconditional: this is the call that leaves the cursor.
    apply(chunk)
}

/// The kinds a restore reads nothing from, for a caller reporting what it
/// skipped.
///
/// `digests` is verification metadata about Mithril immutable files; ADR-004 is
/// explicit that nothing is written to the stores from it.
pub const UNRESTORED_KINDS: [&str; 1] = [DIGESTS];

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::{inscription::LayerDescriptor, Digest};

    use super::*;
    use crate::MAINNET_MAGIC;

    fn descriptor(kind: &str, scope: serde_json::Value, byte: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: format!("application/vnd.dolos.stele.{kind}.v1+zstd"),
            diff_id: Digest::from_bytes([byte; 32]),
            records: 1,
            uncompressed_size: 100,
            scope,
        }
    }

    fn epoch_descriptor(kind: &str, epoch: u64, byte: u8) -> LayerDescriptor {
        descriptor(
            kind,
            json!({
                "epoch": epoch,
                "startSlot": epoch * 100,
                "endSlot": epoch * 100 + 99,
            }),
            byte,
        )
    }

    fn inscription(layers: Vec<LayerDescriptor>) -> Inscription {
        let mut inscription = Inscription::new(
            &DolosProfile,
            3,
            crate::position(
                &crate::Network::for_magic(MAINNET_MAGIC),
                &ChainPoint::Specific(250, [0x0b; 32].into()),
                2,
            )
            .unwrap(),
            crate::parameters(),
            crate::compression(),
        );

        inscription.layers = layers;
        inscription
    }

    fn state_shards() -> Vec<LayerDescriptor> {
        (0..STATE_SHARDS)
            .map(|shard| descriptor(STATE, json!({ "shard": shard }), shard as u8))
            .collect()
    }

    #[test]
    fn every_shard_of_the_state_tip_is_required() {
        select_state(&inscription(state_shards())).unwrap();

        let mut short = state_shards();
        short.pop();

        let err = select_state(&inscription(short)).unwrap_err();
        assert!(matches!(err, Error::IncompleteStele(_)), "{err:?}");

        let mut doubled = state_shards();
        doubled.push(descriptor(STATE, json!({"shard": 0}), 0xff));

        let err = select_state(&inscription(doubled)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    #[test]
    fn epoch_layers_group_by_the_epoch_their_scope_names() {
        let layers = vec![
            epoch_descriptor(BLOCKS, 0, 1),
            epoch_descriptor(BLOCKS, 1, 2),
            epoch_descriptor(LOGS, 0, 3),
            epoch_descriptor(INDEXES, 1, 4),
        ];

        let epochs = select_epochs(&inscription(layers)).unwrap();

        assert_eq!(epochs.len(), 2);

        assert_eq!(epochs[0].epoch, 0);
        assert!(epochs[0].blocks.is_some());
        assert!(epochs[0].logs.is_some());
        assert!(epochs[0].indexes.is_none(), "a kind nobody published");

        assert_eq!(epochs[1].epoch, 1);
        assert_eq!((epochs[1].start_slot, epochs[1].end_slot), (100, 199));
    }

    #[test]
    fn one_epoch_described_twice_is_refused() {
        let doubled = vec![
            epoch_descriptor(BLOCKS, 0, 1),
            epoch_descriptor(BLOCKS, 0, 2),
        ];

        let err = select_epochs(&inscription(doubled)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");

        // And so is one epoch carrying two different slot windows.
        let mut disagreeing = vec![epoch_descriptor(BLOCKS, 0, 1)];
        disagreeing.push(descriptor(
            LOGS,
            json!({"epoch": 0, "startSlot": 0, "endSlot": 50}),
            2,
        ));

        let err = select_epochs(&inscription(disagreeing)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    #[test]
    fn a_scope_without_an_epoch_is_refused() {
        let nonsense = vec![descriptor(BLOCKS, json!({"shard": 0}), 1)];

        let err = select_epochs(&inscription(nonsense)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// `max_history` is a slot window measured back from the tip, so what
    /// survives is what reaches into it — not a count of epochs.
    #[test]
    fn max_history_drops_the_epochs_below_its_window() {
        let epochs: Vec<EpochLayers> = (0..3)
            .map(|epoch| EpochLayers {
                epoch,
                start_slot: epoch * 100,
                end_slot: epoch * 100 + 99,
                blocks: None,
                logs: None,
                indexes: None,
            })
            .collect();

        let (kept, skipped) = retain_history(epochs.clone(), 250, None);
        assert_eq!(kept.len(), 3);
        assert_eq!(skipped, 0);

        // 150 slots back from 250 is slot 100, which epoch 1 covers and epoch 0
        // does not.
        let (kept, skipped) = retain_history(epochs.clone(), 250, Some(150));
        assert_eq!(kept.iter().map(|e| e.epoch).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(skipped, 1);

        // A window wider than the chain keeps everything; a window of nothing
        // still keeps the epoch the tip stands in.
        assert_eq!(
            retain_history(epochs.clone(), 250, Some(u64::MAX)).0.len(),
            3
        );
        assert_eq!(
            retain_history(epochs, 250, Some(0))
                .0
                .iter()
                .map(|e| e.epoch)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn the_selected_size_is_the_selected_layers_and_not_the_document() {
        let mut layers = state_shards();
        layers.push(epoch_descriptor(BLOCKS, 0, 0xa0));
        layers.push(epoch_descriptor(BLOCKS, 2, 0xa2));

        let stele = inscription(layers);
        let epochs = select_epochs(&stele).unwrap();
        let (epochs, skipped) = retain_history(epochs, 250, Some(100));

        let plan = Plan {
            position: read_position(&stele.position).unwrap(),
            sequence: stele.sequence,
            epochs,
            state: select_state(&stele).unwrap(),
            skipped_epochs: skipped,
        };

        assert_eq!(plan.skipped_epochs, 1);
        assert_eq!(plan.layers().count(), STATE_SHARDS as usize + 1);
        assert_eq!(plan.uncompressed_size(), (STATE_SHARDS + 1) * 100);
        assert!(plan.uncompressed_size() < stele.uncompressed_size());
    }

    /// A node asked to restore another chain's stele stops before it opens a
    /// store, not after it has written half a ledger.
    #[test]
    fn a_foreign_network_is_refused_by_the_plan() {
        let stele = inscription(state_shards());
        let position = read_position(&stele.position).unwrap();

        assert_eq!(position.network.magic(), MAINNET_MAGIC);

        // `plan` needs a directory; the refusal it makes is this comparison,
        // which is checked here against the same reader the plan uses.
        assert_ne!(position.network.magic(), crate::PREPROD_MAGIC);
    }

    #[test]
    fn a_layer_header_naming_another_network_is_refused() {
        use crate::{EpochScope, Scope as _};

        let scope = EpochScope {
            network_magic: MAINNET_MAGIC,
            epoch: 0,
            start_slot: 0,
            end_slot: 99,
        };

        let header = LayerHeader::new(crate::PROFILE_NAME, BLOCKS, scope.header().unwrap());
        let descriptor = epoch_descriptor(BLOCKS, 0, 1);

        check_layer_magic(&descriptor, &header, MAINNET_MAGIC).unwrap();

        let err = check_layer_magic(&descriptor, &header, crate::PREVIEW_MAGIC).unwrap_err();
        assert!(
            matches!(
                err,
                Error::NetworkMismatch {
                    expected: crate::PREVIEW_MAGIC,
                    found: MAINNET_MAGIC
                }
            ),
            "{err:?}"
        );
    }

    /// Every scope shape leads with the magic, so one reader covers all three.
    #[test]
    fn the_magic_check_reads_every_scope_shape() {
        use crate::{DigestsScope, EpochScope, Scope as _, StateScope};

        let shapes: Vec<(&str, stelae::CanonicalCbor)> = vec![
            (
                BLOCKS,
                EpochScope {
                    network_magic: MAINNET_MAGIC,
                    epoch: 1,
                    start_slot: 2,
                    end_slot: 3,
                }
                .header()
                .unwrap(),
            ),
            (
                STATE,
                StateScope {
                    network_magic: MAINNET_MAGIC,
                    epoch: 1,
                    shard: 9,
                }
                .header()
                .unwrap(),
            ),
            (
                DIGESTS,
                DigestsScope {
                    network_magic: MAINNET_MAGIC,
                    epoch: 1,
                    last_immutable: 42,
                }
                .header()
                .unwrap(),
            ),
        ];

        for (kind, scope) in shapes {
            let header = LayerHeader::new(crate::PROFILE_NAME, kind, scope);
            let descriptor = descriptor(kind, json!({}), 1);

            check_layer_magic(&descriptor, &header, MAINNET_MAGIC).unwrap();
            assert!(
                check_layer_magic(&descriptor, &header, 42).is_err(),
                "{kind}"
            );
        }
    }

    /// The preflight is a refusal for a doomed run, not a promise about a
    /// tight one: an impossible requirement is refused and a plausible one is
    /// allowed through.
    #[test]
    fn the_preflight_refuses_what_cannot_fit() {
        let temp = tempfile::tempdir().unwrap();

        let mut plan = Plan {
            position: read_position(&inscription(vec![]).position).unwrap(),
            sequence: 3,
            epochs: Vec::new(),
            state: state_shards(),
            skipped_epochs: 0,
        };

        plan.preflight(temp.path()).unwrap();

        // A directory that does not exist yet is measured through its parent,
        // which is the shape a fresh node's storage path has.
        plan.preflight(&temp.path().join("not").join("created").join("yet"))
            .unwrap();

        for descriptor in &mut plan.state {
            descriptor.uncompressed_size = u64::MAX / STATE_SHARDS;
        }

        let err = plan.preflight(temp.path()).unwrap_err();
        assert!(matches!(err, Error::IncompleteStele(_)), "{err:?}");
    }
}
