use dolos_core::{config::RootConfig, BlockSlot, ChainPoint, RawBlock, WalStore};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use miette::{Context, IntoDiagnostic};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraBlock;

struct Feedback {
    _multi: MultiProgress,
    global_pb: ProgressBar,
}

impl Default for Feedback {
    fn default() -> Self {
        let multi = MultiProgress::new();

        let global_pb = ProgressBar::new_spinner();
        let global_pb = multi.add(global_pb);
        global_pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        Self {
            _multi: multi,
            global_pb,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {}

/// A chain discontinuity found while scanning the WAL.
#[derive(Debug)]
struct IntegrityIssue {
    slot: BlockSlot,
    block_hash: Option<Hash<32>>,
    expected_prev: Option<Hash<32>>,
    got_prev: Option<Hash<32>>,
}

/// Walks an iterator of WAL block entries and verifies the prev-hash chain.
///
/// Synthetic entries written by `WalStore::reset_to` (empty block bytes and/or
/// points that are not fully defined) are skipped; `last_hash` is reset to
/// `None` after a skip so the chain check restarts cleanly from the next real
/// block. Discontinuities are collected and returned rather than aborting at
/// the first mismatch, so the caller can report every issue in one pass.
fn scan_chain_integrity(
    blocks: impl Iterator<Item = (ChainPoint, RawBlock)>,
    mut on_progress: impl FnMut(BlockSlot),
) -> miette::Result<Vec<IntegrityIssue>> {
    let mut issues = Vec::new();
    let mut last_hash: Option<Hash<32>> = None;

    for (point, block) in blocks {
        let slot = point.slot();
        on_progress(slot);

        // Skip synthetic entries produced by `reset_to`: their `block` bytes
        // are empty (LogValue::origin()) and/or the point isn't fully defined.
        // Reset the chain so the next real block starts a fresh link check.
        if block.is_empty() || !point.is_fully_defined() {
            last_hash = None;
            continue;
        }

        let blockd = MultiEraBlock::decode(&block)
            .into_diagnostic()
            .with_context(|| format!("decoding block at slot {slot}"))?;

        if let Some(expected_prev) = last_hash {
            let got_prev = blockd.header().previous_hash();
            if got_prev != Some(expected_prev) {
                issues.push(IntegrityIssue {
                    slot,
                    block_hash: point.hash(),
                    expected_prev: Some(expected_prev),
                    got_prev,
                });
            }
        }

        last_hash = point.hash();
    }

    Ok(issues)
}

fn fmt_hash(h: Option<Hash<32>>) -> String {
    match h {
        Some(h) => hex::encode(h),
        None => "none".to_string(),
    }
}

pub fn run(config: &RootConfig, _args: &Args) -> miette::Result<()> {
    let feedback = Feedback::default();

    let wal = crate::common::open_wal_store(config)?;

    let (tip, _) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    feedback.global_pb.set_length(tip.slot());

    let blocks = wal
        .iter_blocks(None, None)
        .into_diagnostic()
        .context("crawling wal")?;

    let pb = feedback.global_pb.clone();
    let issues = scan_chain_integrity(blocks, |slot| {
        pb.set_position(slot);
        pb.set_message(format!("checking slot {slot}"));
    })?;

    if issues.is_empty() {
        println!("no integrity issues found in wal");
        return Ok(());
    }

    for issue in &issues {
        eprintln!(
            "discontinuity at slot {}: block {}, expected prev {}, got prev {}",
            issue.slot,
            fmt_hash(issue.block_hash),
            fmt_hash(issue.expected_prev),
            fmt_hash(issue.got_prev),
        );
    }

    Err(miette::miette!(
        "found {} integrity issue(s) in wal",
        issues.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::Cbor;
    use pallas::{
        codec::utils::{Bytes, KeepRaw},
        ledger::{
            primitives::{conway::{Block, Header, HeaderBody, OperationalCert}, VrfCert},
            traverse::{ComputeHash, Era},
        },
    };
    use std::sync::Arc;

    /// Build a minimal Conway block with an explicit `prev_hash` and return
    /// its `(ChainPoint, RawBlock)`. The block hash is derived from the header
    /// so callers can chain blocks by passing the previous block's hash.
    fn make_block_with_prev(slot: BlockSlot, prev_hash: Option<Hash<32>>) -> (ChainPoint, RawBlock) {
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

        let issues = scan_chain_integrity(entries.into_iter(), |_| {}).unwrap();
        assert!(issues.is_empty(), "chained blocks after reset should pass");
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

        let issues = scan_chain_integrity(entries.into_iter(), |_| {}).unwrap();
        assert!(issues.is_empty(), "origin reset + blocks should pass");
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

        let issues = scan_chain_integrity(entries.into_iter(), |_| {}).unwrap();
        assert_eq!(issues.len(), 1, "exactly one discontinuity expected");

        let issue = &issues[0];
        assert_eq!(issue.slot, 102);
        assert_eq!(issue.expected_prev, Some(h1));
        assert_eq!(issue.got_prev, Some(wrong));
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

        let issues = scan_chain_integrity(entries.into_iter(), |_| {}).unwrap();
        assert!(
            issues.is_empty(),
            "reset_to in the middle should reset the chain, not cause issues"
        );
    }
}
