//! Restoring from a registry whose transfers drop.
//!
//! A restore reads its layers through `stelae::oci`, which retries a read
//! round trip that fails transiently, a body cut off partway included. These
//! tests hold the restore a node runs to that: opened through
//! [`registry::open`] with [`registry::Tuning::default`], as `dolos bootstrap
//! stelae` opens it, against an in-process registry that cuts a layer off.
//! No container, so they run under plain `cargo test`. The same registry,
//! fault-free, holds the paired restore to the outcomes of its blocks
//! repository.

mod fault_registry;
mod node;
mod paired;
mod watcher;

use std::{sync::Arc, time::Instant};

use dolos_core::{ArchiveStore, Domain as _, StateStore as _};
use dolos_snapshot::{
    registry::{self, Point},
    restore, Error,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
use fault_registry::FaultRegistry;
use node::{harness, plan_at_boundary, Blank};
use stelae::{
    oci::{Auth, Options, Registry, DEFAULT_READ_ATTEMPTS, DEFAULT_READ_DELAY},
    progress::Observer,
};
use watcher::Watcher;

const REPOSITORY: &str = "dolos/faults";

/// Where the blocks snapshot the published snapshot names lives.
const BLOCKS_REPOSITORY: &str = "dolos/faults-blocks";

/// Install `ring` as the process-default crypto provider, which `stelae::oci`
/// asks of whatever process opens a `Registry`.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("nothing else installed a provider first");
    });
}

/// A node's stores, published once into `registry`, the closed epoch's blocks
/// beside the snapshot in [`BLOCKS_REPOSITORY`].
fn published(
    registry: &FaultRegistry,
    scratch: &std::path::Path,
) -> (ToyDomain<MemoryStores>, u64) {
    let domain = harness::<MemoryStores>();
    let magic = u64::from(domain.genesis().network_magic());
    let plan = plan_at_boundary(&domain, 1);

    let root = tempfile::tempdir().unwrap();
    dolos_snapshot::export::publish(
        root.path(),
        &plan,
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap();

    paired::pair(
        root.path(),
        &opened(registry, scratch),
        &opened_at(registry, scratch, BLOCKS_REPOSITORY),
    );

    (domain, magic)
}

/// The repository opened as `dolos bootstrap stelae` opens it.
fn opened(registry: &FaultRegistry, scratch: &std::path::Path) -> Registry {
    opened_at(registry, scratch, REPOSITORY)
}

/// The repository `name`, opened the same way.
fn opened_at(registry: &FaultRegistry, scratch: &std::path::Path, name: &str) -> Registry {
    install_crypto_provider();

    registry::open(
        &registry.repository(name),
        true,
        Auth::default(),
        scratch.to_path_buf(),
        registry::Tuning::default(),
    )
    .unwrap()
}

fn restoring(storage: &std::path::Path, magic: u64) -> restore::Restoring<'_> {
    restore::Restoring {
        network_magic: magic,
        max_history: None,
        storage_path: storage,
        resume: false,
        skip_space_check: false,
        journals: 0,
        read_ahead: None,
        source: None,
    }
}

fn restore_into(
    (source, blocks): (&Registry, &Registry),
    blank: &Blank<MemoryStores>,
    storage: &std::path::Path,
    magic: u64,
    observer: &Observer,
) -> Result<restore::Summary, Error> {
    registry::restore_registry(
        source,
        Point::Latest,
        Some(blocks),
        restoring(storage, magic),
        restore::Target::new(&blank.archive, blank.state()),
        observer,
    )
    .map(|(_, _, summary)| summary)
}

/// A layer cut off partway through its first transfer is fetched again, after
/// the default read wait, and the restore completes as if nothing happened.
///
/// The wait is what shows the node's default tuning carries the read budget:
/// the publish budget would have retried after 500 ms.
#[test]
fn a_restore_survives_a_layer_cut_off_mid_transfer() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (domain, magic) = published(&registry, scratch.path());

    let cut = registry.largest_layer();
    registry.cut_next(&cut, 1);

    let blank = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();
    let watcher = Arc::new(Watcher::default());

    let started = Instant::now();
    let summary = restore_into(
        (
            &opened(&registry, scratch.path()),
            &opened_at(&registry, scratch.path(), BLOCKS_REPOSITORY),
        ),
        &blank,
        storage.path(),
        magic,
        &watcher.observer(),
    )
    .unwrap_or_else(|e| panic!("the restore gave up on one dropped transfer: {e}"));
    let elapsed = started.elapsed();

    assert_eq!(registry.gets(&cut), 2, "the cut layer is fetched twice");
    assert_eq!(watcher.retries(), 1, "exactly one retry is announced");
    assert!(
        elapsed >= DEFAULT_READ_DELAY,
        "the retry waited {elapsed:?}, not the default read delay {DEFAULT_READ_DELAY:?}"
    );

    assert_eq!(summary.layers_fetched, registry.layers().len());

    assert_eq!(
        blank.state().read_cursor().unwrap(),
        domain.state().read_cursor().unwrap(),
        "the cursor the node published from"
    );

    assert_eq!(
        blocks_of(&blank.archive),
        blocks_of(domain.archive()),
        "the blocks the node published"
    );
}

fn blocks_of<A: ArchiveStore>(store: &A) -> Vec<(u64, Vec<u8>)> {
    store.get_range(None, None).unwrap().collect()
}

/// A layer that never arrives whole fails the restore with the transport's
/// exhaustion, which names the blob, the attempt count and the last cause.
///
/// Over a transport with the waits shortened, so the six attempts take
/// milliseconds; the count is the default.
#[test]
fn an_exhausted_layer_read_names_the_blob_and_the_attempts() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (_, magic) = published(&registry, scratch.path());

    let cut = registry.largest_layer();
    registry.cut_always(&cut);

    let quick = |name| {
        Registry::open(
            &registry.repository(name),
            Options {
                insecure: true,
                scratch_dir: Some(scratch.path().to_path_buf()),
                read_delay: std::time::Duration::from_millis(1),
                ..Options::default()
            },
        )
        .unwrap()
    };
    let (source, blocks) = (quick(REPOSITORY), quick(BLOCKS_REPOSITORY));

    let blank = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();

    let error = restore_into(
        (&source, &blocks),
        &blank,
        storage.path(),
        magic,
        &Observer::silent(),
    )
    .expect_err("a layer that never arrives whole cannot be restored");

    assert!(
        matches!(
            &error,
            Error::Stelae(stelae::Error::RetriesExhausted { attempts, .. })
                if *attempts == DEFAULT_READ_ATTEMPTS
        ),
        "{error:?}"
    );

    let message = error.to_string();

    assert!(message.contains(&cut), "the digest is named: {message}");
    assert!(
        message.contains(&format!("after {DEFAULT_READ_ATTEMPTS} attempt(s)")),
        "the attempt count is named: {message}"
    );
    assert!(
        message.contains("end of file before message length reached"),
        "the last cause is named: {message}"
    );

    assert_eq!(
        registry.gets(&cut),
        DEFAULT_READ_ATTEMPTS as usize,
        "every attempt fetched the layer again"
    );
}

/// Restore with `fault` armed, and return what the observer saw retried.
///
/// The reads before the first layer (the manifest and the config blob) run
/// before the restore has a snapshot to watch, so this is what shows the
/// observer is attached to the registry, not only to the snapshot it yields.
fn retries_seen_with(fault: impl FnOnce(&FaultRegistry)) -> (FaultRegistry, usize) {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (_, magic) = published(&registry, scratch.path());

    fault(&registry);

    let blank = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();
    let watcher = Arc::new(Watcher::default());

    let summary = restore_into(
        (
            &opened(&registry, scratch.path()),
            &opened_at(&registry, scratch.path(), BLOCKS_REPOSITORY),
        ),
        &blank,
        storage.path(),
        magic,
        &watcher.observer(),
    )
    .unwrap_or_else(|e| panic!("the restore gave up on one failed read: {e}"));

    assert_eq!(summary.layers_fetched, registry.layers().len());

    let retries = watcher.retries();

    (registry, retries)
}

/// A config blob cut off once is fetched again, and the retry is announced to
/// the restore's observer.
#[test]
fn a_retried_config_read_is_announced() {
    let (registry, retries) = retries_seen_with(|registry| {
        registry.cut_next(&registry.config(), 1);
    });

    // Twice by the head read that gives the blocks walk its sequence, then
    // once by the resolve.
    assert_eq!(
        registry.gets(&registry.config()),
        3,
        "the cut config is fetched again"
    );
    assert_eq!(retries, 1, "the config retry reaches the observer");
}

/// A manifest the registry fails once is fetched again, and the retry is
/// announced to the restore's observer.
#[test]
fn a_retried_manifest_read_is_announced() {
    // The publish reads the moving tag too, so only the restore's are counted.
    let published = std::cell::Cell::new(0);

    let (registry, retries) = retries_seen_with(|registry| {
        published.set(registry.manifest_gets());
        registry.fail_next_manifest(1);
    });

    // The head read that gives the blocks walk its sequence: the diff manifest
    // twice, then the tip manifest beside it. Then the resolve's two, and the
    // blocks snapshot's, which has no tip.
    assert_eq!(
        registry.manifest_gets() - published.get(),
        6,
        "the failed diff manifest is fetched again"
    );
    assert_eq!(retries, 1, "the manifest retry reaches the observer");
}

/// A resume whose recorded snapshot the registry won't serve is refused as gone
/// only when the registry says the tag is absent. Any other failure, here a
/// refused read, is reported as itself, since starting over with `--force`
/// is not the fix for it.
#[test]
fn a_recorded_resume_is_gone_only_when_its_tag_is_absent() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let source = restore::Source::Repo(registry.repository(REPOSITORY));

    restore::SourceRecord::new(&source, 7, stelae::Digest::from_bytes([7; 32]))
        .unwrap()
        .save(&restore::source_path_in(storage.path()))
        .unwrap();

    let resume = || {
        let blank = Blank::<MemoryStores>::open();

        registry::restore_registry(
            &opened(&registry, scratch.path()),
            Point::Epoch(7),
            None,
            restore::Restoring {
                resume: true,
                source: Some(&source),
                ..restoring(storage.path(), 0)
            },
            restore::Target::new(&blank.archive, blank.state()),
            &Observer::silent(),
        )
        .unwrap_err()
    };

    // Nothing is published, so the registry answers the tag is unknown.
    let error = resume();
    assert!(
        matches!(error, Error::Resume(restore::ResumeRefusal::Gone(_))),
        "{error:?}"
    );

    registry.deny_next_manifest(1);

    let error = resume();
    assert!(!matches!(error, Error::Resume(_)), "{error:?}");
    assert!(error.to_string().contains("denied"), "{error}");
}

/// The boundary [`chained`] cuts its head at; epochs 0 to 2 are closed.
const HEAD: u64 = 3;

/// The harness published on boundaries 1 to [`HEAD`] into a network root,
/// then paired into [`REPOSITORY`] and the blocks repository `blocks`, so both
/// chains have links to walk. The root is kept for a directory restore.
fn chained(
    registry: &FaultRegistry,
    scratch: &std::path::Path,
    blocks: &str,
) -> (
    ToyDomain<MemoryStores>,
    u64,
    paired::Paired,
    tempfile::TempDir,
) {
    let domain = harness::<MemoryStores>();
    let magic = u64::from(domain.genesis().network_magic());
    let root = tempfile::tempdir().unwrap();

    for epoch in 1..=HEAD {
        dolos_snapshot::export::publish(
            root.path(),
            &plan_at_boundary(&domain, epoch),
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
        .unwrap();
    }

    let pair = paired::pair(
        root.path(),
        &opened(registry, scratch),
        &opened_at(registry, scratch, blocks),
    );

    (domain, magic, pair, root)
}

/// Restore [`REPOSITORY`]'s `latest`, its blocks from [`BLOCKS_REPOSITORY`],
/// keeping `max_history` slots of history.
fn restore_chained(
    registry: &FaultRegistry,
    scratch: &std::path::Path,
    blank: &Blank<MemoryStores>,
    magic: u64,
    max_history: Option<u64>,
) -> Result<(restore::Plan, restore::Summary), Error> {
    let storage = tempfile::tempdir().unwrap();

    registry::restore_registry(
        &opened(registry, scratch),
        Point::Latest,
        Some(&opened_at(registry, scratch, BLOCKS_REPOSITORY)),
        restore::Restoring {
            max_history,
            ..restoring(storage.path(), magic)
        },
        restore::Target::new(&blank.archive, blank.state()),
        &Observer::silent(),
    )
    .map(|(plan, _, summary)| (plan, summary))
}

/// A paired restore resolves the Dolos and blocks chains and writes what a
/// directory restore of the same network root writes, blocks repository
/// included.
#[test]
fn a_paired_restore_resolves_both_chains_as_a_directory_restore_does() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (domain, magic, pair, root) = chained(&registry, scratch.path(), BLOCKS_REPOSITORY);

    let blank = Blank::<MemoryStores>::open();
    let (plan, summary) = restore_chained(&registry, scratch.path(), &blank, magic, None)
        .unwrap_or_else(|e| panic!("the paired restore failed: {e}"));

    assert_eq!(plan.blocks, Some(pair.blocks[&HEAD]));
    assert_eq!(blocks_of(&blank.archive), blocks_of(domain.archive()));
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        domain.state().read_cursor().unwrap(),
    );

    let control = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();
    let blocks = node::blocks(root.path());
    let by_directory = restore::execute(
        restore::Input::Directory {
            path: &node::dolos(root.path()),
            blocks: blocks.as_deref(),
        },
        restoring(storage.path(), magic),
        restore::Target::new(&control.archive, control.state()),
        None,
    )
    .unwrap();

    assert_eq!(summary, by_directory.summary);
}

/// A history window that keeps no closed epoch reads no blocks repository, so
/// one holding no snapshot at the head's sequence does not fail the restore,
/// though its chain was walked beside the Dolos one.
#[test]
fn a_window_that_needs_no_blocks_ignores_an_empty_blocks_repository() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (domain, magic, _, _root) = chained(&registry, scratch.path(), "dolos/elsewhere");

    let blank = Blank::<MemoryStores>::open();
    let (plan, summary) = restore_chained(&registry, scratch.path(), &blank, magic, Some(0))
        .unwrap_or_else(|e| panic!("a restore that needs no blocks failed: {e}"));

    assert_eq!(plan.blocks, None);
    assert_eq!(summary.blocks, blocks_of(&blank.archive).len() as u64);
    assert!(summary.blocks > 0, "the boundary window's blocks");
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        domain.state().read_cursor().unwrap(),
    );
}

/// A restore that needs the closed epochs' blocks and finds no snapshot at the
/// head's sequence in the blocks repository is refused, naming the snapshot
/// the head references, before anything is written.
#[test]
fn a_restore_that_needs_blocks_and_finds_none_is_refused() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (_, magic, pair, _root) = chained(&registry, scratch.path(), "dolos/elsewhere");

    let blank = Blank::<MemoryStores>::open();
    let error = restore_chained(&registry, scratch.path(), &blank, magic, None)
        .map(|(plan, _)| plan.blocks)
        .unwrap_err();

    let Error::BlocksMissing { reference, .. } = &error else {
        panic!("{error:?}");
    };

    assert_eq!(*reference, pair.blocks[&HEAD]);
    assert!(
        error
            .to_string()
            .contains(&format!("holds no snapshot at epoch-{HEAD}")),
        "{error}"
    );
    assert!(blank.state().read_cursor().unwrap().is_none());
    assert!(blocks_of(&blank.archive).is_empty());
}

/// A restore of a checked snapshot reads what the check resolved and nothing
/// else: no manifest is read again, so tags dropped after the check, the
/// moving one included, are not read, and it writes what a restore that
/// resolves its own point writes.
#[test]
fn a_checked_restore_reads_the_checked_snapshot_and_no_manifest() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (domain, magic, pair, _root) = chained(&registry, scratch.path(), BLOCKS_REPOSITORY);

    let checked = registry::check(
        &opened(&registry, scratch.path()),
        Point::Latest,
        Some(&opened_at(&registry, scratch.path(), BLOCKS_REPOSITORY)),
        None,
        &Observer::silent(),
    )
    .unwrap_or_else(|e| panic!("the check failed: {e}"));

    assert_eq!(checked.sequence(), HEAD);

    registry.untag_all(REPOSITORY);
    registry.untag_all(BLOCKS_REPOSITORY);
    let checked_at = registry.manifest_gets();

    let blank = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();
    let outcome = restore::execute(
        restore::Input::Checked(&checked),
        restoring(storage.path(), magic),
        restore::Target::new(&blank.archive, blank.state()),
        None,
    )
    .unwrap_or_else(|e| panic!("the checked restore failed: {e}"));

    assert_eq!(
        registry.manifest_gets(),
        checked_at,
        "a manifest was read again"
    );
    assert_eq!(outcome.plan.sequence, HEAD);
    assert_eq!(outcome.plan.blocks, Some(pair.blocks[&HEAD]));
    assert_eq!(blocks_of(&blank.archive), blocks_of(domain.archive()));
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        domain.state().read_cursor().unwrap(),
    );

    // With the tags gone, a restore that resolves its point finds nothing.
    let error = restore_chained(&registry, scratch.path(), &Blank::open(), magic, None)
        .map(|(plan, _)| plan.sequence)
        .unwrap_err();

    assert!(error.to_string().contains("latest"), "{error}");
}

/// A manifest the registry fails once during the check is fetched again, and
/// the retry is announced to the observer the check was given, once: the
/// checked restore reads no manifest, so it has none to retry.
#[test]
fn a_retried_manifest_read_during_the_check_is_announced() {
    let registry = FaultRegistry::spawn();
    let scratch = tempfile::tempdir().unwrap();
    let (domain, magic, _, _root) = chained(&registry, scratch.path(), BLOCKS_REPOSITORY);
    let watcher = Arc::new(Watcher::default());

    registry.fail_next_manifest(1);

    let checked = registry::check(
        &opened(&registry, scratch.path()),
        Point::Latest,
        Some(&opened_at(&registry, scratch.path(), BLOCKS_REPOSITORY)),
        None,
        &watcher.observer(),
    )
    .unwrap_or_else(|e| panic!("the check gave up on one failed read: {e}"));

    assert_eq!(
        watcher.retries(),
        1,
        "the check's retry reaches the observer"
    );

    let blank = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();
    let observer = watcher.observer();

    restore::execute(
        restore::Input::Checked(&checked),
        restoring(storage.path(), magic),
        restore::Target::new(&blank.archive, blank.state()),
        Some(&observer),
    )
    .unwrap_or_else(|e| panic!("the checked restore failed: {e}"));

    assert_eq!(watcher.retries(), 1, "the retry was announced again");
    assert_eq!(blocks_of(&blank.archive), blocks_of(domain.archive()));
}
