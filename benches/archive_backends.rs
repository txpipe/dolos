//! Regression guards for the archive backend's three read-shape families.
//!
//! The store is populated with the synthetic content from
//! `dolos_testing::archive` and measured against a fixed key sample: block
//! location resolution and full block reads (`get_block_by_slot`), the
//! per-account reward point reads (`read_logs` per epoch), and the per-epoch
//! temporal-prefix scan (`iter_logs`).
//!
//! These are *relative* regression guards on small tempdir populations.
//! Absolute numbers say nothing about production behavior — the
//! backend-adoption evaluation ran against full preprod replays — and under
//! `cargo test` each bench body runs exactly once as a smoke pass.

use dolos_core::{
    archive::Skippable as _, ArchiveStore as CoreArchiveStore, NamespaceType, StateSchema,
};
use dolos_testing::archive::{populate_archive, ArchiveShape};

/// The one namespace the log shapes exercise; the name is the real bulk
/// namespace so the bench reads like the node.
const NS: &str = "account-epochs";

const SHAPE: ArchiveShape = ArchiveShape {
    epochs: 4,
    blocks_per_epoch: 250,
    log_rows_per_epoch: 1000,
    slots_per_epoch: 432_000,
    seed: 0xA5C1_1BEE,
};

/// Sampled read keys per timed iteration: every 5th block (200 of 1000) and
/// a 32-account stride across the 1000 entities.
const BLOCK_SAMPLE_STRIDE: usize = 5;
const ACCOUNT_SAMPLES: u64 = 32;

fn schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert(NS, NamespaceType::KeyValue);
    schema
}

/// One archive backend under measurement, mirroring the conformance suite's
/// seam (`tests/archive_conformance.rs`). The seam is kept generic even with
/// one persistent backend left, so the shapes stay comparable across a
/// future one.
trait Backend {
    type Store: CoreArchiveStore;

    fn open(dir: &std::path::Path) -> Self::Store;
}

struct Fjall;

impl Backend for Fjall {
    type Store = dolos_fjall::archive::ArchiveStore;

    fn open(dir: &std::path::Path) -> Self::Store {
        // Single worker thread so CI runners are not oversubscribed; the
        // rest are the backend's own defaults.
        let config = dolos_core::config::FjallArchiveConfig {
            cache: Some(16),
            flush_on_commit: Some(false),
            worker_threads: Some(1),
            ..Default::default()
        };

        dolos_fjall::archive::ArchiveStore::open(schema(), dir, &config)
            .expect("failed to open fjall archive store")
    }
}

fn populated<B: Backend>() -> (B::Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let store = B::open(dir.path());
    populate_archive(&store, NS, &SHAPE).expect("failed to populate archive");
    (store, dir)
}

fn sampled_slots() -> Vec<u64> {
    SHAPE.block_slots().step_by(BLOCK_SAMPLE_STRIDE).collect()
}

#[divan::bench(types = [Fjall], sample_count = 50)]
fn block_location_resolution<B: Backend>(bencher: divan::Bencher) {
    let (store, _dir) = populated::<B>();
    let slots = sampled_slots();

    // skip_forward silently stops at exhaustion, so an empty range would
    // "resolve" nothing and still time successfully; pin every sampled slot
    // to a real block before the timed passes.
    for &slot in &slots {
        assert!(
            store.get_block_by_slot(&slot).unwrap().is_some(),
            "sampled slot missing from the population"
        );
    }

    bencher.bench_local(|| {
        for &slot in &slots {
            let mut iter = store.get_range(Some(slot), Some(slot + 1)).unwrap();
            iter.skip_forward(1);
            divan::black_box(&iter);
        }
    });
}

#[divan::bench(types = [Fjall], sample_count = 50)]
fn block_full_read<B: Backend>(bencher: divan::Bencher) {
    let (store, _dir) = populated::<B>();
    let slots = sampled_slots();

    bencher.bench_local(|| {
        for &slot in &slots {
            let body = store.get_block_by_slot(&slot).unwrap();
            assert!(
                divan::black_box(body).is_some(),
                "sampled slot lost its block"
            );
        }
    });
}

#[divan::bench(types = [Fjall], sample_count = 50)]
fn rewards_point_reads<B: Backend>(bencher: divan::Bencher) {
    let (store, _dir) = populated::<B>();
    let account_stride = SHAPE.log_rows_per_epoch / ACCOUNT_SAMPLES;

    bencher.bench_local(|| {
        for account in 0..ACCOUNT_SAMPLES {
            for epoch in 0..SHAPE.epochs {
                let key = SHAPE.log_key(epoch, account * account_stride);
                let hits = store.read_logs(NS, &[&key]).unwrap();
                assert!(divan::black_box(hits)[0].is_some(), "sampled row missing");
            }
        }
    });
}

#[divan::bench(types = [Fjall], sample_count = 50)]
fn epoch_scan<B: Backend>(bencher: divan::Bencher) {
    let (store, _dir) = populated::<B>();
    let scanned_epoch = SHAPE.epochs / 2;
    let range = SHAPE.log_key(scanned_epoch, 0)..SHAPE.log_key(scanned_epoch + 1, 0);

    bencher.bench_local(|| {
        let mut rows = 0u64;
        for row in store.iter_logs(NS, range.clone()).unwrap() {
            divan::black_box(row.unwrap());
            rows += 1;
        }
        assert_eq!(rows, SHAPE.log_rows_per_epoch, "epoch drain lost rows");
    });
}

fn main() {
    divan::main();
}
