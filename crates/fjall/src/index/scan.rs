//! The prefix walk both archive record traversals are.
//!
//! `iter_archive_tags` and `iter_exact_records` are the same shape: neither
//! can find its own labels on disk (the stored key holds a *hash* of the
//! dimension or kind, not its name), so both are driven by a known label list,
//! seeking one prefix at a time and letting the label fall out of which prefix
//! is open. Both then filter by slot, and both feed a signed snapshot layer,
//! which is what fixes their error policy.
//!
//! This module is that walk, once. What the two traversals actually differ at
//! is [`ScanTarget`]: four methods' worth.
//!
//! ## Errors are terminal
//!
//! **This is the single definition site of the fuse-on-error policy.** After
//! yielding an `Err` — a read failure or a malformed entry — a scan is fused
//! and yields `None` forever. The record set feeds a signed snapshot layer, so
//! resuming past a fault (or silently skipping a bad entry) would let a corrupt
//! store produce a clean-looking, truncated layer. A consumer that collects
//! into `Result<Vec<_>, _>` therefore cannot receive a silently truncated
//! record set.
//!
//! ## Laziness
//!
//! Construction reads nothing, and a scan holds one entry at a time: the MVCC
//! snapshot and one open prefix iterator, whatever the size of the store.

use std::ops::Range;

use dolos_core::{BlockSlot, IndexError};
use fjall::{Guard, Keyspace, Readable, Snapshot};

use crate::keys::DIM_HASH_SIZE;

/// What a [`DimensionScan`] needs in order to turn one stored entry into one
/// record.
///
/// The four methods are exactly the four points the tag and exact traversals
/// diverge at; everything else about the walk is shared. Implementations live
/// beside the key encoding they decode, since that is what they are about.
pub trait ScanTarget {
    /// What one prefix is opened for — a dimension name, or an exact kind.
    ///
    /// The label is not recoverable from the entries under its prefix, so the
    /// scan carries it and hands it back to [`ScanTarget::decode`]. This is
    /// why traversal is driven by a list rather than by a blind keyspace scan.
    type Label: Copy;

    /// The record yielded for an in-range entry.
    type Record;

    /// The keyspace prefix every entry labelled `label` lives under.
    fn prefix(label: Self::Label) -> [u8; DIM_HASH_SIZE];

    /// Decode one entry.
    ///
    /// `Ok(None)` means the entry is outside `slots` and the scan should keep
    /// going. `Err` is terminal — see this module's error policy — and covers
    /// both read failures and malformed entries.
    fn decode(
        label: Self::Label,
        guard: Guard,
        slots: &Range<BlockSlot>,
    ) -> Result<Option<Self::Record>, IndexError>;
}

/// A lazy walk over the entries of a label list's prefixes, in list order.
///
/// Ordering within a prefix is fjall's lexicographic key order, which each
/// [`ScanTarget`]'s key encoding is chosen to make the rest of its traversal
/// contract. Ordering *across* prefixes is the order `labels` yields them in,
/// so a caller that owes a sorted contract sorts the list before building the
/// scan.
pub struct DimensionScan<T: ScanTarget, L: Iterator<Item = T::Label>> {
    snapshot: Snapshot,
    keyspace: Keyspace,
    labels: L,
    current: Option<(T::Label, fjall::Iter)>,
    slots: Range<BlockSlot>,
    done: bool,
}

impl<T: ScanTarget, L: Iterator<Item = T::Label>> DimensionScan<T, L> {
    /// Build a scan over `labels`, yielding only entries whose slot falls in
    /// the half-open range `slots`.
    ///
    /// Returns immediately without reading any data.
    pub fn new(
        snapshot: Snapshot,
        keyspace: &Keyspace,
        labels: L,
        slots: Range<BlockSlot>,
    ) -> Self {
        Self {
            snapshot,
            keyspace: keyspace.clone(),
            labels,
            current: None,
            // An empty range matches nothing; skip the prefix scans entirely.
            done: slots.is_empty(),
            slots,
        }
    }

    /// Open a prefix scan for the next label, if any is left.
    fn open_next(&mut self) -> Option<()> {
        let label = self.labels.next()?;
        let iter = self.snapshot.prefix(&self.keyspace, T::prefix(label));
        self.current = Some((label, iter));
        Some(())
    }
}

impl<T: ScanTarget, L: Iterator<Item = T::Label>> Iterator for DimensionScan<T, L> {
    type Item = Result<T::Record, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some((label, iter)) = self.current.as_mut() else {
                self.open_next()?;
                continue;
            };

            let label = *label;

            let Some(guard) = iter.next() else {
                self.current = None;
                continue;
            };

            match T::decode(label, guard, &self.slots) {
                Ok(Some(record)) => return Some(Ok(record)),
                Ok(None) => continue,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl<T: ScanTarget, L: Iterator<Item = T::Label>> std::iter::FusedIterator for DimensionScan<T, L> {}
