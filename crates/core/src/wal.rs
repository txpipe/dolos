use itertools::Itertools as _;

use super::*;

#[trait_variant::make(Send)]
pub trait WalStore: Clone + Send + Sync + 'static {
    type Delta: EntityDelta;
    type LogIterator<'a>: DoubleEndedIterator<Item = LogEntry<Self::Delta>> + Sized + Sync + Send;
    type BlockIterator<'a>: DoubleEndedIterator<Item = (ChainPoint, RawBlock)> + Sized + Sync + Send;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError>;

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError>;

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

    for (point, block) in blocks {
        let slot = point.slot();
        on_progress(slot);

        if block.is_empty() || !point.is_fully_defined() {
            last_hash = None;
            continue;
        }

        let blockd = match pallas::ledger::traverse::MultiEraBlock::decode(&block) {
            Ok(blockd) => blockd,
            Err(error) => {
                report.failures.push(IntegrityFailure::Undecodable {
                    slot,
                    error: error.to_string(),
                });
                report.complete = false;
                break;
            }
        };

        report.blocks += 1;

        if let Some(expected_prev) = last_hash {
            let got_prev = blockd.header().previous_hash();
            if got_prev != Some(expected_prev) {
                report.failures.push(IntegrityFailure::Discontinuity {
                    slot,
                    block_hash: point.hash(),
                    expected_prev: Some(expected_prev),
                    got_prev,
                });
            }
        }

        last_hash = point.hash();
    }

    report
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
