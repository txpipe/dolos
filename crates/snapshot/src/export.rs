//! Walking a live Dolos store set into a stele.
//!
//! Everything the byte shapes need is already frozen: the five codecs in
//! [`crate::layers`], the scopes and `position`/`parameters` builders in
//! [`crate`], and the record order each store iterator already promises. This
//! module is the part that has to *drive* them — the one place that knows which
//! query answers which layer, and in what order.
//!
//! ## The two facts only an exporter can know
//!
//! **Epoch geometry.** A stele's `sequence` is the epoch its cursor has just
//! entered, and its layers cover every epoch below that. [`Plan`] derives both
//! from a [`ChainSummary`] and a cursor, so two publishers standing at the same
//! chain point compute the same windows — including for the final, possibly
//! partial epoch, whose slot window is clamped to the cursor rather than to the
//! epoch boundary. Landing on a true boundary is the publisher pipeline's job,
//! not this module's.
//!
//! **The network name.** It rides inside the canonical JSON, so it is inside
//! the stele's identity. [`crate::Network::for_magic`] is the only way to build
//! one, which is what keeps two publishers on one chain from producing two
//! digests over a spelling.
//!
//! ## Memory
//!
//! Nothing here holds a layer. Records go into a [`RecordSink`] one at a time,
//! and the state pass keeps all sixteen shard sinks open across a single walk
//! of the store rather than sorting a mainnet-sized set into buckets first.
//!
//! ## Where the stele ends up is not this module's business
//!
//! [`export`] takes any [`SteleWriter`] — a directory, a registry, whatever
//! comes next — because nothing in the walk of a store depends on it. The two
//! calls it makes are "open a sink for this layer" and "seal the stele with
//! this inscription", and both are the same code whether the bytes land in
//! `blobs/sha256/` or in a repository.
//!
//! ## What is deliberately not here
//!
//! No `digests` layer is *sourced* — [`export`] writes one when handed the
//! records, but obtaining them means a Mithril aggregator and a certificate
//! check, which is publisher plumbing. No history: an inscription is permitted
//! an empty one at any sequence, and filling it belongs to the slice that has a
//! registry to read. No restore, no signatures.

use std::ops::Range;

use dolos_cardano::{
    eras::ChainSummary, indexes::archive_dimensions, pallas::ledger::traverse::MultiEraBlock,
};
use dolos_core::{
    ArchiveStore, BlockSlot, ChainPoint, EntityKey, IndexStore, LogKey, StateStore, TemporalKey,
};
use stelae::{
    inscription::{Inscription, LayerDescriptor},
    transport::{LayerSpec, RecordSink, SteleWriter},
};

use crate::{
    layers::{blocks, digests, indexes, logs, state},
    namespaces::NAMESPACES,
    DigestsScope, DolosProfile, EpochScope, Error, Network, Scope, StateScope, BLOCKS,
    COMPRESSION_LEVEL, DIGESTS, INDEXES, LOGS, STATE, STATE_SHARDS, UTXOS,
};

/// The slot window one epoch layer covers.
///
/// `end_slot` is inclusive, because that is what the scope publishes and what a
/// reader prints. Every store iterator takes a half-open range, which is
/// [`EpochWindow::slots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochWindow {
    pub epoch: u64,
    pub start_slot: BlockSlot,
    pub end_slot: BlockSlot,
}

impl EpochWindow {
    /// The half-open range every store iterator in the seam takes.
    pub fn slots(&self) -> Range<BlockSlot> {
        self.start_slot..self.end_slot + 1
    }

    pub fn scope(&self, network_magic: u64) -> EpochScope {
        EpochScope {
            network_magic,
            epoch: self.epoch,
            start_slot: self.start_slot,
            end_slot: self.end_slot,
        }
    }
}

/// What a publish covers: where the node stands, and which epochs its layers
/// describe.
///
/// Built before anything is written, so `--dry-run` and the export itself agree
/// by construction rather than by two readings of the same rules.
#[derive(Debug, Clone)]
pub struct Plan {
    pub network: Network,
    /// The chain point the stele stands at — the state store's cursor.
    pub cursor: ChainPoint,
    /// The protocol's `sequence`: the epoch the cursor has just entered.
    pub sequence: u64,
    /// The epochs whose layers this export writes, ascending.
    pub epochs: Vec<EpochWindow>,
}

impl Plan {
    /// Derive the geometry of a publish standing at `cursor`.
    ///
    /// The stele's `sequence` is `epoch_of(cursor) + 1` — ADR-004's E, the
    /// newly started epoch — and its layers cover `0..=epoch_of(cursor)`.
    /// The last window's `end_slot` is clamped to the cursor, so a stele
    /// cut mid-epoch is still byte-identical between two publishers
    /// standing at the same point.
    pub fn new(
        summary: &ChainSummary,
        network: Network,
        cursor: ChainPoint,
    ) -> Result<Self, Error> {
        // Refused here rather than at `position()`, so a plan that could never
        // produce an inscription is not first walked over a mainnet store.
        if cursor.hash().is_none() {
            return Err(Error::UnanchoredPoint(cursor.to_string()));
        }

        let tip_slot = cursor.slot();
        let (tip_epoch, _) = summary.slot_epoch(tip_slot);

        let epochs = (0..=tip_epoch)
            .map(|epoch| EpochWindow {
                epoch,
                start_slot: summary.epoch_start(epoch),
                // The epoch's own last slot, or the cursor if the stele was cut
                // before the boundary.
                end_slot: (summary.epoch_start(epoch + 1) - 1).min(tip_slot),
            })
            .collect();

        Ok(Self {
            network,
            cursor,
            sequence: tip_epoch + 1,
            epochs,
        })
    }

    /// Keep only the epoch layers within `first..=last`.
    ///
    /// A partial publish: the state tip is unaffected, since it is the tip
    /// whatever history travels with it. Bounds outside the covered range
    /// simply select nothing there.
    pub fn restrict_epochs(mut self, first: Option<u64>, last: Option<u64>) -> Self {
        self.epochs.retain(|window| {
            first.is_none_or(|first| window.epoch >= first)
                && last.is_none_or(|last| window.epoch <= last)
        });

        self
    }

    /// The profile's `position` for this plan.
    pub fn position(&self) -> Result<serde_json::Value, Error> {
        // `sequence` names the epoch just entered; `position.epoch` names the
        // one the cursor is standing in, which is the last one the layers cover.
        crate::position(&self.network, &self.cursor, self.sequence - 1)
    }

    /// The immutable tag this stele would publish under.
    pub fn tag(&self) -> Result<String, Error> {
        Ok(stelae::profile::checked_tag_for_sequence(
            &DolosProfile,
            self.sequence,
        )?)
    }

    fn state_scope(&self, shard: u8) -> StateScope {
        StateScope {
            network_magic: self.network.magic(),
            epoch: self.sequence - 1,
            shard,
        }
    }
}

/// Build the publish plan from a live state store.
///
/// The magic is the caller's: it is a property of the node's configuration and
/// genesis, not of anything in the state. The *name* is not — see
/// [`Network::for_magic`].
pub fn plan<S: StateStore>(state: &S, network_magic: u64) -> Result<Plan, Error> {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;

    let cursor = state
        .read_cursor()?
        .ok_or_else(|| Error::UnanchoredPoint("the state store has no cursor".to_owned()))?;

    Plan::new(&summary, Network::for_magic(network_magic), cursor)
}

/// Export a complete stele into `stele`: every layer, then the inscription.
///
/// Layers are listed in [`crate::KINDS`] order, and within a kind in ascending
/// epoch or shard order. That order is part of the canonical document, so it is
/// frozen by a golden rather than left to the loop that happens to write them.
///
/// `digest_records` is optional and has no source in this slice — a stele
/// without a `digests` layer is valid, since ADR-004 makes every layer
/// individually non-mandatory.
pub fn export<W, A, S, I>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
) -> Result<Inscription, Error>
where
    W: SteleWriter,
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let mut layers = Vec::new();

    for window in &plan.epochs {
        layers.push(write_blocks(stele, plan, archive, window)?);
    }

    for window in &plan.epochs {
        layers.push(write_indexes(stele, plan, indexes, window)?);
    }

    for window in &plan.epochs {
        layers.push(write_logs(stele, plan, archive, window)?);
    }

    layers.extend(write_state(stele, plan, state)?);

    if let Some(records) = digest_records {
        layers.push(write_digests(stele, plan, records)?);
    }

    let mut inscription = Inscription::new(
        &DolosProfile,
        plan.sequence,
        plan.position()?,
        crate::parameters(),
        crate::compression(),
    );

    inscription.layers = layers;

    // Validated before it is written, so a stele is never sealed over a
    // document that has no digest.
    stele.seal(&DolosProfile, &inscription)?;

    Ok(inscription)
}

/// Create a stele directory at `root` and export into it.
///
/// The front door for a caller that has no stele in hand — the CLI, above all —
/// so the profile stays the only thing in the binary that names the protocol
/// crate. Refuses a directory that already holds an inscription: republishing
/// over a stele in place would leave its old blobs behind, indistinguishable
/// from the new ones.
pub fn publish<A, S, I>(
    root: impl Into<std::path::PathBuf>,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let stele = stelae::dir::SteleDir::create(root)?;

    export(&stele, plan, archive, state, indexes, digest_records)
}

fn sink<W: SteleWriter>(stele: &W, spec: &LayerSpec) -> Result<W::Sink, Error> {
    Ok(stele.layer_sink(&DolosProfile, spec, COMPRESSION_LEVEL)?)
}

/// One epoch's blocks, ascending slot.
///
/// The hash is not stored beside the block, so it is derived here by decoding
/// the header — the codec takes it as an input and neither derives nor checks
/// it, which makes this the one site that has to be right.
fn write_blocks<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    window: &EpochWindow,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());
    let mut sink = sink(stele, &scope.layer_spec(BLOCKS)?)?;
    let mut order = blocks::OrderCheck::default();

    let slots = window.slots();

    for (slot, body) in archive.get_range(Some(slots.start), Some(slots.end))? {
        let decoded = MultiEraBlock::decode(&body).map_err(|e| Error::UndecodableBlock {
            slot,
            reason: e.to_string(),
        })?;

        let record = blocks::BlockRecord::new(slot, decoded.hash(), body);

        order.check(&record)?;
        sink.write_record(&blocks::encode(&record)?)?;
    }

    Ok(sink.finish()?.descriptor)
}

/// One epoch's ledger logs, ordered by `(ns, log_key)`.
///
/// Walking [`NAMESPACES`] in order *is* that ordering: the registry is sorted
/// and each namespace's iterator is key-ascending, so no second registry of
/// "namespaces that have logs" is needed — one with none simply yields nothing.
fn write_logs<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    window: &EpochWindow,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());
    let mut sink = sink(stele, &scope.layer_spec(LOGS)?)?;
    let mut order = logs::OrderCheck::default();

    let slots = window.slots();
    let range = log_key_range(&slots);

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        for entry in archive.iter_logs(ns, range.clone())? {
            let (key, value) = entry?;
            let record = logs::LogRecord::new(ns, key, value);

            order.check(&record)?;
            sink.write_record(&logs::encode(&record)?)?;
        }
    }

    Ok(sink.finish()?.descriptor)
}

/// A log key range covering every log whose slot falls in `slots`.
///
/// A [`LogKey`] built from a [`TemporalKey`] is the slot followed by 32 zero
/// bytes, so the pair brackets exactly the half-open slot window.
fn log_key_range(slots: &Range<BlockSlot>) -> Range<LogKey> {
    LogKey::from(TemporalKey::from(slots.start))..LogKey::from(TemporalKey::from(slots.end))
}

/// One epoch's index records: every tag, then every exact record.
///
/// Both runs come out of the store in the order the layer requires, which the
/// trait promises and the `OrderCheck` holds it to.
///
/// ## This is a scan, not a seek
///
/// Neither traversal can seek to a slot — a tag's slot is the last component of
/// its key and an exact record's slot is its stored *value* — so one epoch
/// layer costs a pass over the whole index store, and a publish of N epochs
/// costs N of them. Measured at 800 ms per epoch against an eight-epoch store
/// and 3.0 s against a thirty-two-epoch one; the extrapolation to mainnet and
/// the banded traversal that answers it are in
/// `plans/dolos-stelae-publish-cost.md`. It is stated here because a reader of
/// this loop should not have to rediscover it.
fn write_indexes<W: SteleWriter, I: IndexStore>(
    stele: &W,
    plan: &Plan,
    store: &I,
    window: &EpochWindow,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());
    let mut sink = sink(stele, &scope.layer_spec(INDEXES)?)?;
    let mut order = indexes::OrderCheck::default();

    let slots = window.slots();

    for tag in store.iter_archive_tags(&archive_dimensions::ALL, slots.clone())? {
        let record = tag?.into();

        order.check(&record)?;
        sink.write_record(&indexes::encode(&record)?)?;
    }

    for exact in store.iter_exact_records(slots)? {
        let record = exact?.into();

        order.check(&record)?;
        sink.write_record(&indexes::encode(&record)?)?;
    }

    Ok(sink.finish()?.descriptor)
}

/// The state tip, as sixteen shards written in one pass.
///
/// All sixteen sinks stay open while the store is walked once and each record
/// is routed by [`crate::shard_of`]. The alternative — sixteen passes, or one
/// pass buffering into sixteen buckets — is either sixteen full scans of a
/// mainnet-sized state or the whole tip in memory.
///
/// Every shard is written, including an empty one, so the shard count a reader
/// sees is [`STATE_SHARDS`] and never a function of what the data happened to
/// contain.
fn write_state<W: SteleWriter, S: StateStore>(
    stele: &W,
    plan: &Plan,
    store: &S,
) -> Result<Vec<LayerDescriptor>, Error> {
    let shards = 0..STATE_SHARDS as u8;

    let mut sinks = Vec::with_capacity(shards.len());
    let mut orders = Vec::with_capacity(shards.len());

    for shard in shards {
        sinks.push(sink(stele, &plan.state_scope(shard).layer_spec(STATE)?)?);
        orders.push(state::OrderCheck::for_shard(shard));
    }

    // `NAMESPACES` is sorted and `utxos` is its last entry, so one walk in
    // registry order yields `(ns, key)` ascending within every shard — the
    // layer's ordering rule, for free.
    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        // `full_range` is the store's own name for everything, and it ends
        // *exclusively* at `[0xff; 32]` — so an entity keyed with thirty-two
        // `0xff` bytes is invisible to it. That is a limit of a fixed-width key
        // type rather than something export could route around: there is no
        // representable exclusive bound above the maximum key. It is stated
        // rather than silently inherited, because a state entity that no
        // publisher can export is worth someone knowing about.
        for entry in store.iter_entities(ns, EntityKey::full_range())? {
            let (key, value) = entry?;

            route(&mut sinks, &mut orders, state::entity(ns, &key, &value)?)?;
        }
    }

    for entry in store.iter_utxos()? {
        let (txo, value) = entry?;

        route(&mut sinks, &mut orders, state::utxo(&txo, &value)?)?;
    }

    sinks
        .into_iter()
        .map(|sink| Ok(sink.finish()?.descriptor))
        .collect()
}

/// Send one state record to the shard its key belongs to.
///
/// The shard's own `OrderCheck` is what catches a routing mistake. Without it a
/// misrouted record still restores — the write path dispatches on the
/// namespace, not the shard — and only a client fetching shards selectively
/// would ever notice it was missing.
fn route<K: RecordSink>(
    sinks: &mut [K],
    orders: &mut [state::OrderCheck],
    record: state::StateRecord,
) -> Result<(), Error> {
    let shard = record.shard() as usize;

    orders[shard].check(&record)?;
    sinks[shard].write_record(&state::encode(&record)?)?;

    Ok(())
}

/// The optional `digests` layer.
///
/// `lastImmutable` is read off the records rather than taken as a second input:
/// it is the last immutable file the layer covers, so deriving it removes the
/// only way the scope and the records could disagree.
fn write_digests<W: SteleWriter>(
    stele: &W,
    plan: &Plan,
    records: &[digests::ImmutableDigests],
) -> Result<LayerDescriptor, Error> {
    let mut order = digests::OrderCheck::default();

    for record in records {
        order.check(record)?;
    }

    let last_immutable = records
        .last()
        .ok_or_else(|| {
            Error::malformed(
                DIGESTS,
                "a digests layer with no records names no immutable file; omit the layer instead",
            )
        })?
        .immutable_number;

    let scope = DigestsScope {
        network_magic: plan.network.magic(),
        epoch: plan.sequence - 1,
        last_immutable,
    };

    let mut sink = sink(stele, &scope.layer_spec(DIGESTS)?)?;

    for record in records {
        sink.write_record(&digests::encode(record)?)?;
    }

    Ok(sink.finish()?.descriptor)
}

#[cfg(test)]
mod tests {
    use dolos_core::BlockHash;

    use super::*;

    /// A summary with one era: 100-slot epochs starting at slot 0.
    fn summary() -> ChainSummary {
        use dolos_cardano::model::EraSummary;
        use dolos_cardano::EraBoundary;

        let mut chain = ChainSummary::default();

        chain.append_era(
            6,
            EraSummary {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: 100,
                slot_length: 1,
                protocol: 6,
            },
        );

        chain
    }

    fn point(slot: u64) -> ChainPoint {
        ChainPoint::Specific(slot, BlockHash::new([0xab; 32]))
    }

    #[test]
    fn the_sequence_is_the_epoch_the_cursor_has_just_entered() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250)).unwrap();

        assert_eq!(plan.sequence, 3);
        assert_eq!(plan.epochs.len(), 3);
        assert_eq!(plan.position().unwrap()["epoch"], 2u64);
    }

    /// A stele cut mid-epoch clamps its last window to the cursor, so two
    /// publishers standing at the same point publish the same windows.
    #[test]
    fn the_last_window_is_clamped_to_the_cursor() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250)).unwrap();

        assert_eq!(
            plan.epochs,
            vec![
                EpochWindow {
                    epoch: 0,
                    start_slot: 0,
                    end_slot: 99
                },
                EpochWindow {
                    epoch: 1,
                    start_slot: 100,
                    end_slot: 199
                },
                EpochWindow {
                    epoch: 2,
                    start_slot: 200,
                    end_slot: 250
                },
            ]
        );

        // On a true boundary the clamp is a no-op.
        let plan = Plan::new(&summary(), Network::for_magic(2), point(299)).unwrap();
        assert_eq!(plan.epochs.last().unwrap().end_slot, 299);
    }

    #[test]
    fn every_window_is_half_open_when_a_store_reads_it() {
        let window = EpochWindow {
            epoch: 1,
            start_slot: 100,
            end_slot: 199,
        };

        assert_eq!(window.slots(), 100..200);
        assert_eq!(
            log_key_range(&window.slots()).start.as_ref()[..8],
            100u64.to_be_bytes()
        );
        assert_eq!(
            log_key_range(&window.slots()).end.as_ref()[..8],
            200u64.to_be_bytes()
        );
    }

    #[test]
    fn an_unanchored_cursor_has_no_plan() {
        for unanchored in [ChainPoint::Origin, ChainPoint::Slot(250)] {
            let err = Plan::new(&summary(), Network::for_magic(2), unanchored).unwrap_err();
            assert!(matches!(err, Error::UnanchoredPoint(_)), "{err:?}");
        }
    }

    #[test]
    fn restricting_epochs_keeps_the_window_bounds() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250))
            .unwrap()
            .restrict_epochs(Some(1), Some(1));

        assert_eq!(
            plan.sequence, 3,
            "the sequence is the cursor's, not the cut"
        );
        assert_eq!(plan.epochs.len(), 1);
        assert_eq!(plan.epochs[0].epoch, 1);
    }
}
