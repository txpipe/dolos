//! Deterministic archive populations for benchmarks and stress tests.
//!
//! [`populate_archive`] writes the same blocks and log rows into any
//! [`ArchiveStore`] implementation, so two backends populated from the same
//! [`ArchiveShape`] hold identical content and can be measured against the
//! same keys. Randomness comes from an inlined SplitMix64 over the shape's
//! seed rather than `rand`, so a population is reproducible from the shape
//! alone.

use std::sync::Arc;

use dolos_core::{
    ArchiveError, ArchiveStore, ArchiveWriter as _, BlockSlot, ChainPoint, EntityKey, LogKey,
    Namespace, TemporalKey,
};

/// SplitMix64: tiny, seedable, and good enough to size filler payloads.
#[derive(Clone)]
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The dimensions of a synthetic archive population.
///
/// Blocks are laid out at evenly strided slots within each epoch; log rows
/// are written per epoch under that epoch's starting slot, one row for each
/// of `log_rows_per_epoch` entities. Entity keys depend only on the entity
/// index, so the same entities recur across epochs — the layout the
/// per-account reward reads walk in production.
#[derive(Clone)]
pub struct ArchiveShape {
    pub epochs: u64,
    pub blocks_per_epoch: u64,
    pub log_rows_per_epoch: u64,
    pub slots_per_epoch: u64,
    pub seed: u64,
}

impl ArchiveShape {
    pub fn epoch_start(&self, epoch: u64) -> BlockSlot {
        epoch * self.slots_per_epoch
    }

    /// Every block slot the population writes, in ascending order. Callers
    /// sample read keys from this instead of scanning the store, so both
    /// backends measure exactly the keys that exist.
    pub fn block_slots(&self) -> impl Iterator<Item = BlockSlot> + '_ {
        let stride = (self.slots_per_epoch / self.blocks_per_epoch).max(1);

        (0..self.epochs).flat_map(move |epoch| {
            (0..self.blocks_per_epoch).map(move |i| self.epoch_start(epoch) + i * stride)
        })
    }

    /// The stable key of entity `i`, identical in every epoch.
    pub fn entity(&self, i: u64) -> EntityKey {
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_exact_mut(8) {
            chunk.copy_from_slice(&i.to_be_bytes());
        }
        EntityKey::from(&bytes)
    }

    /// The log key of entity `i` at `epoch`'s boundary.
    pub fn log_key(&self, epoch: u64, i: u64) -> LogKey {
        LogKey::from((TemporalKey::from(self.epoch_start(epoch)), self.entity(i)))
    }
}

fn block_point(slot: BlockSlot) -> ChainPoint {
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_exact_mut(8) {
        chunk.copy_from_slice(&slot.to_be_bytes());
    }
    ChainPoint::Specific(slot, pallas::crypto::hash::Hash::new(bytes))
}

fn filler(rng: &mut SplitMix64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Write `shape` into `store`: one committed writer per epoch carrying that
/// epoch's blocks (~1-2 KiB bodies) and its log rows (~30-60 B values) under
/// `ns`, which must exist in the store's schema.
pub fn populate_archive<S: ArchiveStore>(
    store: &S,
    ns: Namespace,
    shape: &ArchiveShape,
) -> Result<(), ArchiveError> {
    let mut rng = SplitMix64(shape.seed);
    let stride = (shape.slots_per_epoch / shape.blocks_per_epoch).max(1);

    for epoch in 0..shape.epochs {
        let writer = store.start_writer()?;
        let epoch_start = shape.epoch_start(epoch);

        for i in 0..shape.blocks_per_epoch {
            let slot = epoch_start + i * stride;
            let len = 1024 + (rng.next_u64() % 1024) as usize;
            writer.apply(&block_point(slot), &Arc::new(filler(&mut rng, len)))?;
        }

        for i in 0..shape.log_rows_per_epoch {
            let len = 30 + (rng.next_u64() % 31) as usize;
            writer.write_log(ns, &shape.log_key(epoch, i), &filler(&mut rng, len))?;
        }

        writer.commit()?;
    }

    Ok(())
}
