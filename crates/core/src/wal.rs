use itertools::Itertools as _;

use super::*;

#[trait_variant::make(Send)]
pub trait WalStore: Clone + Send + Sync + 'static {
    type Delta: EntityDelta;
    type LogIterator<'a>: DoubleEndedIterator<Item = LogEntry<Self::Delta>> + Sized + Sync + Send;
    type BlockIterator<'a>: DoubleEndedIterator<Item = (ChainPoint, RawBlock)> + Sized + Sync + Send;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError>;

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError>;

    /// Remove the entries at `points` in one commit, skipping any the WAL
    /// does not hold.
    fn remove_points(&self, points: &[ChainPoint]) -> Result<(), WalError>;

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError>;

    fn locate_point(&self, around: BlockSlot) -> Result<Option<ChainPoint>, WalError>;

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<Self::Delta>>, WalError>;

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::LogIterator<'a>, WalError>;

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError>;

    fn read_sparse(
        &self,
        points: &[ChainPoint],
    ) -> Result<Vec<Option<LogValue<Self::Delta>>>, WalError> {
        points.iter().map(|p| self.read_entry(p)).try_collect()
    }

    fn append_entries(&self, logs: Vec<LogEntry<Self::Delta>>) -> Result<(), WalError>;

    fn remove_entries(&mut self, after: &ChainPoint) -> Result<(), WalError>;

    fn contains_point(&self, point: &ChainPoint) -> Result<bool, WalError> {
        let entry = self.read_entry(point)?;
        Ok(entry.is_some())
    }

    /// Asserts that a chain point exists in the WAL and returns the sequence
    ///
    /// Similar to `locate_point` but it expects a point to be found or
    /// otherwise return a NotFound error.
    fn assert_point(&self, point: &ChainPoint) -> Result<(), WalError> {
        let contains = self.contains_point(point)?;

        if !contains {
            return Err(WalError::PointNotFound(point.clone()));
        }

        Ok(())
    }

    fn iter_all<'a>(&'a self) -> Result<Self::LogIterator<'a>, WalError> {
        self.iter_logs(None, None)
    }

    fn find_start(&self) -> Result<Option<LogEntry<Self::Delta>>, WalError> {
        let start = self.iter_all()?.next();

        Ok(start)
    }

    fn find_tip(&self) -> Result<Option<LogEntry<Self::Delta>>, WalError> {
        let tip = self.iter_all()?.next_back();

        Ok(tip)
    }

    fn intersect_candidates(&self, max_items: usize) -> Result<Vec<ChainPoint>, WalError> {
        let mut iter = self.iter_all()?.rev();

        let mut out = Vec::with_capacity(max_items);

        // crawl the wal exponentially
        while let Some((point, _)) = iter.next() {
            // Skip synthetic entries with zero hash (from reset_to with ChainPoint::Slot)
            if !point.is_fully_defined() {
                continue;
            }

            out.push(point);

            if out.len() >= max_items {
                break;
            }

            // skip exponentially
            let skip = 2usize.pow(out.len() as u32) - 1;
            for _ in 0..skip {
                iter.next();
            }
        }

        Ok(out)
    }

    fn find_intersect(
        &self,
        intersect: &[ChainPoint],
    ) -> Result<Option<LogEntry<Self::Delta>>, WalError> {
        for candidate in intersect {
            if let Some(entry) = self.read_entry(candidate)? {
                return Ok(Some((candidate.clone(), entry)));
            }
        }

        Ok(None)
    }
}

/// The block bytes of a WAL entry, for [`ChainOrdered`].
pub(crate) trait EntryBlock {
    fn block_bytes(&self) -> &[u8];
}

impl<D: EntityDelta> EntryBlock for LogValue<D> {
    fn block_bytes(&self) -> &[u8] {
        &self.block
    }
}

impl EntryBlock for RawBlock {
    fn block_bytes(&self) -> &[u8] {
        self
    }
}

/// WAL entries in chain order, or in reverse chain order over a reversed
/// read. Keys sort the entries of one slot by hash, but a Byron boundary block
/// and the main block that shares its slot chain one after the other: within
/// a slot, a block comes after the entry its parent hash names.
pub(crate) struct ChainOrdered<I: Iterator> {
    entries: std::iter::Peekable<I>,
    pending: std::collections::VecDeque<I::Item>,
    reversed: bool,
}

impl<I, T> ChainOrdered<I>
where
    I: Iterator<Item = (ChainPoint, T)>,
    T: EntryBlock,
{
    pub(crate) fn forward(entries: I) -> Self {
        Self::new(entries, false)
    }

    /// Over a reversed read, as a rollback walks the WAL.
    pub(crate) fn backward(entries: I) -> Self {
        Self::new(entries, true)
    }

    fn new(entries: I, reversed: bool) -> Self {
        Self {
            entries: entries.peekable(),
            pending: Default::default(),
            reversed,
        }
    }
}

impl<I, T> Iterator for ChainOrdered<I>
where
    I: Iterator<Item = (ChainPoint, T)>,
    T: EntryBlock,
{
    type Item = (ChainPoint, T);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.pending.pop_front() {
            return Some(entry);
        }

        let first = self.entries.next()?;
        let slot = first.0.slot();

        let Some(second) = self.entries.next_if(|(point, _)| point.slot() == slot) else {
            return Some(first);
        };

        let mut group = vec![first, second];
        while let Some(entry) = self.entries.next_if(|(point, _)| point.slot() == slot) {
            group.push(entry);
        }

        // Key order first, so entries that don't chain to each other (the
        // Origin entry beside a block at slot 0) keep it.
        if self.reversed {
            group.reverse();
        }

        let mut links: Vec<_> = group
            .into_iter()
            .map(|entry| (parent_hash(entry.1.block_bytes()), entry))
            .collect();

        let mut ordered = Vec::with_capacity(links.len());

        while !links.is_empty() {
            let next = links
                .iter()
                .position(|(parent, _)| {
                    !links
                        .iter()
                        .any(|(_, (point, _))| parent.is_some() && point.hash() == *parent)
                })
                .unwrap_or(0);

            ordered.push(links.remove(next).1);
        }

        if self.reversed {
            ordered.reverse();
        }

        self.pending.extend(ordered);
        self.pending.pop_front()
    }
}

/// The hash a block names as its parent; none for a synthetic entry or bytes
/// that don't decode.
fn parent_hash(block: &[u8]) -> Option<BlockHash> {
    pallas::ledger::traverse::MultiEraBlock::decode(block)
        .ok()
        .and_then(|block| block.header().previous_hash())
}

/// What [`verify_integrity`] found walking a WAL's prev-hash chain.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IntegrityReport {
    /// Whether the scan read every entry: it stops at a block it cannot
    /// decode, and goes on past a discontinuity.
    pub complete: bool,
    /// The blocks checked, synthetic entries aside.
    pub blocks: u64,
    /// Every failure found, in WAL order.
    pub failures: Vec<IntegrityFailure>,
}

impl IntegrityReport {
    pub fn first_failure(&self) -> Option<&IntegrityFailure> {
        self.failures.first()
    }

    pub fn is_intact(&self) -> bool {
        self.complete && self.failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IntegrityFailure {
    /// A block whose previous hash is not the hash of the block before it.
    Discontinuity {
        slot: BlockSlot,
        block_hash: Option<BlockHash>,
        expected_prev: Option<BlockHash>,
        got_prev: Option<BlockHash>,
    },
    /// A block that fails to decode, where the scan stopped.
    Undecodable { slot: BlockSlot, error: String },
}

/// Walk the WAL from its start and check that each block's previous hash is
/// the hash of the block before it.
pub fn verify_integrity<W: WalStore>(wal: &W) -> Result<IntegrityReport, WalError> {
    verify_integrity_with(wal, |_| {})
}

/// [`verify_integrity`], calling `on_progress` with each entry's slot.
pub fn verify_integrity_with<W: WalStore>(
    wal: &W,
    on_progress: impl FnMut(BlockSlot),
) -> Result<IntegrityReport, WalError> {
    Ok(scan_chain_integrity(
        wal.iter_blocks(None, None)?,
        on_progress,
    ))
}

/// Synthetic entries written by `WalStore::reset_to` (empty block bytes
/// and/or points that are not fully defined) are skipped, and the chain check
/// restarts at the next real block.
///
/// The WAL orders the entries of one slot by hash rather than chain order, so
/// a Byron boundary block and the main block that shares its slot can come
/// in either order; the blocks of a slot are checked in the order they chain.
fn scan_chain_integrity(
    blocks: impl Iterator<Item = (ChainPoint, RawBlock)>,
    mut on_progress: impl FnMut(BlockSlot),
) -> IntegrityReport {
    let mut report = IntegrityReport {
        complete: true,
        blocks: 0,
        failures: Vec::new(),
    };
    let mut last_hash: Option<BlockHash> = None;
    let mut slot_links: Vec<Link> = Vec::new();

    for (point, block) in blocks {
        let slot = point.slot();
        on_progress(slot);

        if slot_links.first().is_some_and(|link| link.slot != slot) {
            chain_slot(&mut slot_links, &mut last_hash, &mut report);
        }

        if block.is_empty() || !point.is_fully_defined() {
            chain_slot(&mut slot_links, &mut last_hash, &mut report);
            last_hash = None;
            continue;
        }

        let blockd = match pallas::ledger::traverse::MultiEraBlock::decode(&block) {
            Ok(blockd) => blockd,
            Err(error) => {
                chain_slot(&mut slot_links, &mut last_hash, &mut report);
                report.failures.push(IntegrityFailure::Undecodable {
                    slot,
                    error: error.to_string(),
                });
                report.complete = false;
                return report;
            }
        };

        report.blocks += 1;

        slot_links.push(Link {
            slot,
            hash: point.hash(),
            prev: blockd.header().previous_hash(),
        });
    }

    chain_slot(&mut slot_links, &mut last_hash, &mut report);

    report
}

/// A block's place in the prev-hash chain.
struct Link {
    slot: BlockSlot,
    hash: Option<BlockHash>,
    prev: Option<BlockHash>,
}

/// Check the blocks of one slot against `last_hash`, taking first the block
/// that follows it, else one whose parent is not among them.
fn chain_slot(
    links: &mut Vec<Link>,
    last_hash: &mut Option<BlockHash>,
    report: &mut IntegrityReport,
) {
    while !links.is_empty() {
        let next = links
            .iter()
            .position(|link| last_hash.is_some() && link.prev == *last_hash)
            .or_else(|| {
                links.iter().position(|link| {
                    !links
                        .iter()
                        .any(|other| other.hash.is_some() && other.hash == link.prev)
                })
            })
            .unwrap_or(0);

        let link = links.remove(next);

        if let Some(expected_prev) = *last_hash {
            if link.prev != Some(expected_prev) {
                report.failures.push(IntegrityFailure::Discontinuity {
                    slot: link.slot,
                    block_hash: link.hash,
                    expected_prev: Some(expected_prev),
                    got_prev: link.prev,
                });
            }
        }

        *last_hash = link.hash;
    }
}

#[cfg(test)]
mod integrity_tests {
    use super::*;
    use pallas::{
        codec::utils::{Bytes, KeepRaw},
        crypto::hash::Hash,
        ledger::{
            primitives::{
                conway::{Block, Header, HeaderBody, OperationalCert},
                VrfCert,
            },
            traverse::{ComputeHash, Era},
        },
    };
    use std::sync::Arc;

    /// Build a minimal Conway block with an explicit `prev_hash` and return
    /// its `(ChainPoint, RawBlock)`. The block hash is derived from the header
    /// so callers can chain blocks by passing the previous block's hash.
    fn make_block_with_prev(
        slot: BlockSlot,
        prev_hash: Option<Hash<32>>,
    ) -> (ChainPoint, RawBlock) {
        let header = KeepRaw::from(Header {
            header_body: HeaderBody {
                slot,
                block_body_hash: Hash::from([0u8; 32]),
                block_number: 0,
                prev_hash,
                issuer_vkey: Bytes::from(vec![]),
                vrf_vkey: Bytes::from(vec![]),
                vrf_result: VrfCert(Bytes::from(vec![]), Bytes::from(vec![])),
                block_body_size: 0,
                protocol_version: (1, 0),
                operational_cert: OperationalCert {
                    operational_cert_hot_vkey: Bytes::from(vec![]),
                    operational_cert_sequence_number: 0,
                    operational_cert_kes_period: 0,
                    operational_cert_sigma: Bytes::from(vec![]),
                },
            },
            body_signature: Bytes::from(vec![]),
        });

        let block = Block {
            header,
            transaction_bodies: Default::default(),
            transaction_witness_sets: Default::default(),
            auxiliary_data_set: Default::default(),
            invalid_transactions: Default::default(),
        };

        let hash = block.header.compute_hash();
        let wrapper = (Era::Conway as u16, block);
        let raw_bytes = pallas::codec::minicbor::to_vec(&wrapper).unwrap();
        (ChainPoint::Specific(slot, hash), Arc::new(raw_bytes))
    }

    /// Synthetic WAL entry like the one `WalStore::reset_to` writes: a
    /// fully-defined point with empty block bytes (`LogValue::origin()`).
    fn synthetic_entry(slot: BlockSlot, hash: Hash<32>) -> (ChainPoint, RawBlock) {
        (ChainPoint::Specific(slot, hash), Arc::new(Cbor::new()))
    }

    /// WAL seeded via `reset_to` + appended blocks → tool passes.
    #[test]
    fn passes_after_reset_to_with_chained_blocks() {
        let reset_hash = Hash::from([0xAA; 32]);

        let (p1, b1) = make_block_with_prev(101, None);
        let h1 = p1.hash().unwrap();
        let (p2, b2) = make_block_with_prev(102, Some(h1));
        let h2 = p2.hash().unwrap();
        let (p3, b3) = make_block_with_prev(103, Some(h2));

        let entries: Vec<(ChainPoint, RawBlock)> = vec![
            synthetic_entry(100, reset_hash),
            (p1, b1),
            (p2, b2),
            (p3, b3),
        ];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.is_intact(), "chained blocks after reset should pass");
        assert_eq!(report.blocks, 3);
    }

    /// `reset_to(Origin)` synthetic entry followed by real blocks.
    #[test]
    fn passes_after_reset_to_origin() {
        let (p1, b1) = make_block_with_prev(1, None);
        let h1 = p1.hash().unwrap();
        let (p2, b2) = make_block_with_prev(2, Some(h1));

        let entries: Vec<(ChainPoint, RawBlock)> = vec![
            (ChainPoint::Origin, Arc::new(Cbor::new())),
            (p1, b1),
            (p2, b2),
        ];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.is_intact(), "origin reset + blocks should pass");
    }

    /// WAL with a manufactured discontinuity → tool reports it with context
    /// and continues scanning (does not panic, does not stop at first issue).
    #[test]
    fn reports_discontinuity_with_context() {
        let (p1, b1) = make_block_with_prev(101, None);
        let h1 = p1.hash().unwrap();

        // Block 2 claims a wrong prev_hash → discontinuity.
        let wrong = Hash::from([0xEE; 32]);
        let (p2, b2) = make_block_with_prev(102, Some(wrong));
        let h2 = p2.hash().unwrap();

        // Block 3 correctly chains to block 2 → proves scan continued past
        // the first mismatch.
        let (p3, b3) = make_block_with_prev(103, Some(h2));

        let entries: Vec<(ChainPoint, RawBlock)> = vec![(p1, b1), (p2, b2), (p3, b3)];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.complete);
        assert_eq!(report.blocks, 3);
        assert_eq!(
            report.failures.len(),
            1,
            "exactly one discontinuity expected"
        );
        assert_eq!(
            report.first_failure(),
            Some(&IntegrityFailure::Discontinuity {
                slot: 102,
                block_hash: Some(h2),
                expected_prev: Some(h1),
                got_prev: Some(wrong),
            })
        );
    }

    /// A rollback in the middle of the WAL (synthetic re-seed) resets the
    /// chain check; blocks after the re-seed start a fresh link.
    #[test]
    fn rollback_resets_chain_check() {
        let (p1, b1) = make_block_with_prev(101, None);
        let h1 = p1.hash().unwrap();
        let (p2, b2) = make_block_with_prev(102, Some(h1));

        // Simulate a rollback + reset_to: synthetic entry at slot 50.
        let reset_hash = Hash::from([0xBB; 32]);

        // Blocks after the reset start a fresh chain.
        let (p3, b3) = make_block_with_prev(51, None);
        let h3 = p3.hash().unwrap();
        let (p4, b4) = make_block_with_prev(52, Some(h3));

        let entries: Vec<(ChainPoint, RawBlock)> = vec![
            (p1, b1),
            (p2, b2),
            synthetic_entry(50, reset_hash),
            (p3, b3),
            (p4, b4),
        ];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(
            report.is_intact(),
            "reset_to in the middle should reset the chain, not cause issues"
        );
    }

    /// A Byron boundary block and the main block that shares its slot, which
    /// the WAL orders by hash: the main block can come first, and the pair
    /// still chains.
    #[test]
    fn blocks_sharing_a_slot_chain_in_either_order() {
        let (p1, b1) = make_block_with_prev(101, None);
        let (boundary, bb) = make_block_with_prev(120, p1.hash());
        let (main, bm) = make_block_with_prev(120, boundary.hash());
        let (p3, b3) = make_block_with_prev(121, main.hash());

        let entries: Vec<(ChainPoint, RawBlock)> = vec![
            (p1, b1),
            (main.clone(), bm.clone()),
            (boundary.clone(), bb.clone()),
            (p3.clone(), b3.clone()),
        ];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.blocks, 4);

        // The pair first in the WAL, with no block before it to chain to.
        let entries: Vec<(ChainPoint, RawBlock)> = vec![(main, bm), (boundary, bb), (p3, b3)];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.is_intact(), "{report:?}");
    }

    /// Blocks in different slots are checked in slot order, even when they
    /// would chain in another: B's parent is A and C's is B, but C sits
    /// between them.
    #[test]
    fn blocks_in_different_slots_chain_in_slot_order() {
        let (a, ba) = make_block_with_prev(104, None);
        let (b, bb) = make_block_with_prev(106, a.hash());
        let (c, bc) = make_block_with_prev(105, b.hash());

        let entries: Vec<(ChainPoint, RawBlock)> =
            vec![(a.clone(), ba), (c.clone(), bc), (b.clone(), bb)];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.complete);
        assert_eq!(
            report.failures,
            vec![
                IntegrityFailure::Discontinuity {
                    slot: 105,
                    block_hash: c.hash(),
                    expected_prev: a.hash(),
                    got_prev: b.hash(),
                },
                IntegrityFailure::Discontinuity {
                    slot: 106,
                    block_hash: b.hash(),
                    expected_prev: c.hash(),
                    got_prev: a.hash(),
                },
            ]
        );
    }

    /// Blocks that share a slot and do not chain are still a discontinuity.
    #[test]
    fn blocks_sharing_a_slot_that_do_not_chain_are_reported() {
        let (p1, b1) = make_block_with_prev(101, None);
        let (chained, bc) = make_block_with_prev(120, p1.hash());
        let wrong = Hash::from([0xEE; 32]);
        let (stray, bs) = make_block_with_prev(120, Some(wrong));

        let entries: Vec<(ChainPoint, RawBlock)> =
            vec![(p1, b1), (stray.clone(), bs), (chained.clone(), bc)];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(report.complete);
        assert_eq!(
            report.failures,
            vec![IntegrityFailure::Discontinuity {
                slot: 120,
                block_hash: stray.hash(),
                expected_prev: chained.hash(),
                got_prev: Some(wrong),
            }]
        );
    }

    /// A block that fails to decode stops the scan, which reports itself
    /// incomplete with that block as its last failure.
    #[test]
    fn an_undecodable_block_stops_the_scan() {
        let (p1, b1) = make_block_with_prev(101, None);
        let h1 = p1.hash().unwrap();
        let (p3, b3) = make_block_with_prev(103, Some(Hash::from([0xEE; 32])));

        let entries: Vec<(ChainPoint, RawBlock)> = vec![
            (p1, b1),
            (ChainPoint::Specific(102, h1), Arc::new(vec![0xFF, 0x00])),
            (p3, b3),
        ];

        let report = scan_chain_integrity(entries.into_iter(), |_| {});
        assert!(!report.complete);
        assert_eq!(report.blocks, 1);
        assert!(matches!(
            report.failures.as_slice(),
            [IntegrityFailure::Undecodable { slot: 102, .. }]
        ));
    }

    #[test]
    fn the_report_serializes_with_its_failures() {
        let report = IntegrityReport {
            complete: false,
            blocks: 1,
            failures: vec![IntegrityFailure::Undecodable {
                slot: 7,
                error: "bad".into(),
            }],
        };

        assert_eq!(
            serde_json::to_value(&report).unwrap(),
            serde_json::json!({
                "complete": false,
                "blocks": 1,
                "failures": [{"kind": "undecodable", "slot": 7, "error": "bad"}],
            })
        );
    }
}

#[cfg(test)]
mod chain_order_tests {
    use std::sync::Arc;

    use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev};
    use pallas::crypto::hash::Hash;

    use super::*;

    /// The boundary block opening Byron epoch 1, the main block numbered
    /// `number` chained onto it at the same slot, and the block after both,
    /// in WAL key order behind the Origin entry. `number` picks the main
    /// block's hash, so either of the pair can sort first.
    fn wal(number: u64) -> (Vec<(ChainPoint, RawBlock)>, [ChainPoint; 3]) {
        let slot = byron_ebb_slot(1);
        let (ebb_point, ebb) = make_byron_ebb(1, Hash::new([7u8; 32]));
        let (main_point, main) = make_conway_block_with_prev(slot, ebb_point.hash(), number);
        let (next_point, next) =
            make_conway_block_with_prev(slot + 20, main_point.hash(), number + 1);

        // The fixture's points are the `dolos-core` it links, not this build.
        let ours = |slot, hash: Option<Hash<32>>| ChainPoint::Specific(slot, hash.unwrap());
        let ebb_point = ours(slot, ebb_point.hash());
        let main_point = ours(slot, main_point.hash());
        let next_point = ours(slot + 20, next_point.hash());

        let mut entries = vec![
            (ChainPoint::Origin, Arc::new(vec![])),
            (ebb_point.clone(), ebb),
            (main_point.clone(), main),
            (next_point.clone(), next),
        ];
        entries.sort_by_key(|(point, _)| point.clone().into_bytes());

        (entries, [ebb_point, main_point, next_point])
    }

    fn points(entries: impl Iterator<Item = (ChainPoint, RawBlock)>) -> Vec<ChainPoint> {
        entries.map(|(point, _)| point).collect()
    }

    #[test]
    fn a_boundary_block_comes_before_the_block_sharing_its_slot() {
        let ebb_first = (0..).find(|n| {
            let (_, [ebb, main, _]) = wal(*n);
            ebb.into_bytes() < main.into_bytes()
        });
        let main_first = (0..).find(|n| {
            let (_, [ebb, main, _]) = wal(*n);
            main.into_bytes() < ebb.into_bytes()
        });

        for number in [ebb_first.unwrap(), main_first.unwrap()] {
            let (entries, [ebb, main, next]) = wal(number);
            let chain = vec![ChainPoint::Origin, ebb, main, next];

            assert_eq!(
                points(ChainOrdered::forward(entries.clone().into_iter())),
                chain,
                "main block numbered {number}"
            );

            let mut reversed = chain;
            reversed.reverse();

            assert_eq!(
                points(ChainOrdered::backward(entries.into_iter().rev())),
                reversed,
                "main block numbered {number}"
            );
        }
    }

    /// Entries that don't chain to each other keep their key order: the
    /// Origin entry beside a block at slot 0.
    #[test]
    fn entries_that_do_not_chain_keep_their_key_order() {
        let (point, block) = make_conway_block_with_prev(0, None, 0);
        let point = ChainPoint::Specific(0, point.hash().unwrap());

        let entries = vec![
            (ChainPoint::Origin, Arc::new(vec![])),
            (point.clone(), block),
        ];

        assert_eq!(
            points(ChainOrdered::forward(entries.clone().into_iter())),
            vec![ChainPoint::Origin, point.clone()]
        );
        assert_eq!(
            points(ChainOrdered::backward(entries.into_iter().rev())),
            vec![point, ChainPoint::Origin]
        );
    }

    /// The Origin entry comes first beside a block at slot 0 that names a
    /// parent the WAL doesn't hold, as a Byron chain's first boundary block
    /// names the genesis hash.
    #[test]
    fn origin_comes_before_a_slot_zero_block_naming_a_parent() {
        let (point, block) = make_byron_ebb(0, Hash::new([7u8; 32]));
        let point = ChainPoint::Specific(byron_ebb_slot(0), point.hash().unwrap());

        let entries = vec![
            (ChainPoint::Origin, Arc::new(vec![])),
            (point.clone(), block),
        ];

        assert_eq!(
            points(ChainOrdered::forward(entries.clone().into_iter())),
            vec![ChainPoint::Origin, point.clone()]
        );
        assert_eq!(
            points(ChainOrdered::backward(entries.into_iter().rev())),
            vec![point, ChainPoint::Origin]
        );
    }
}
