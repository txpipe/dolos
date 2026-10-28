//! `Batch::from_archive` must report an unseekable starting point as an error.
//!
//! It used to `panic!("no overlap between archive and wal")` when the archive
//! knew the point but had no body after it, and the WAL could not intersect
//! it. That unwound the caller's task, which killed an in-flight `WatchTx`
//! stream when a client reconnected with a `BlockRef` the WAL no longer
//! covered.
//!
//! Reaching this branch needs a point the *archive* can resolve, so the block
//! below is written to the archive only and the WAL is left untouched.

use dolos_core::{
    crawl::ChainCrawler, indexes::ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _,
    ChainPoint, Domain as _, DomainError,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};
use pallas::ledger::traverse::MultiEraBlock;

/// A domain whose archive resolves `point` but whose WAL has never seen it.
fn domain_with_archived_point() -> (ToyDomain, ChainPoint) {
    let domain = ToyDomain::new(None, None);

    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
    let raw = &blocks[0];
    let block = MultiEraBlock::decode(raw).unwrap();
    let point = ChainPoint::Specific(block.slot(), block.hash());

    let writer = domain.archive().start_writer().unwrap();
    writer.apply(&point, raw).unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot: block.slot(),
            block_hash: block.hash().to_vec(),
            block_number: Some(block.number()),
            tags: vec![],
            tx_hashes: vec![],
        }])
        .unwrap();
    writer.commit().unwrap();

    (domain, point)
}

/// A domain whose archive holds blocks after `start`, but whose WAL never covers
/// any of them: `start` succeeds and the failure only shows up once drained.
fn domain_that_runs_out_mid_crawl() -> (ToyDomain, ChainPoint) {
    let domain = ToyDomain::new(None, None);

    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());

    let writer = domain.archive().start_writer().unwrap();
    for raw in blocks.iter().take(3) {
        let block = MultiEraBlock::decode(raw).unwrap();
        let point = ChainPoint::Specific(block.slot(), block.hash());

        writer.apply(&point, raw).unwrap();
        writer
            .apply_index(&[ArchiveIndexDelta {
                slot: block.slot(),
                block_hash: block.hash().to_vec(),
                block_number: Some(block.number()),
                tags: vec![],
                tx_hashes: vec![],
            }])
            .unwrap();
    }
    writer.commit().unwrap();

    let first = MultiEraBlock::decode(&blocks[0]).unwrap();
    let point = ChainPoint::Specific(first.slot(), first.hash());

    (domain, point)
}

#[test]
fn crawl_that_runs_out_of_overlap_errors_instead_of_ending() {
    let (domain, point) = domain_that_runs_out_mid_crawl();

    let (mut crawler, _) = ChainCrawler::start(&domain, &[point])
        .expect("the archive covers the start point")
        .expect("the archive covers the start point");

    // Drains the two archived blocks that follow the start point.
    assert!(crawler.next_block().unwrap().is_some());
    assert!(crawler.next_block().unwrap().is_some());

    // The WAL never intersects, so the next load reports the gap instead of
    // reporting `None` as if the crawl had reached the tip.
    match crawler.next_block() {
        Err(DomainError::Internal(msg)) => {
            assert!(
                msg.contains("no overlap"),
                "expected a no-overlap error, got: {msg}"
            );
        }
        other => panic!("expected a no-overlap error, got: {other:?}"),
    }

    // The stalled point survives so the caller can re-seek from it.
    assert!(crawler.last_point().is_some());
}

#[test]
fn archive_point_the_wal_cannot_reach_errors_instead_of_panicking() {
    let (domain, point) = domain_with_archived_point();

    // The archive resolves the point, so `start` enters `from_archive`. Its
    // page is the one stored block, which is then skipped, leaving the page
    // empty; the WAL has no overlap. Before the fix this unwound the test.
    match ChainCrawler::start(&domain, &[point]) {
        Err(DomainError::Internal(msg)) => {
            assert!(
                msg.contains("no overlap"),
                "expected a no-overlap error, got: {msg}"
            );
        }
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected an error when the WAL cannot cover the point"),
    }
}
