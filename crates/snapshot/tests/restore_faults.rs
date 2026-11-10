//! Restoring from a registry whose transfers drop.
//!
//! A restore reads its layers through `stelae::oci`, which retries a read
//! round trip that fails transiently, a body cut off partway included. These
//! tests hold the restore a node runs to that: opened through
//! [`registry::open`] with [`registry::Tuning::default`], as `dolos bootstrap
//! stelae` opens it, against an in-process registry that cuts a layer off.
//! No container, so they run under plain `cargo test`.

mod fault_registry;
mod node;
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

/// A node's stores, published once into `registry`.
fn published(
    registry: &FaultRegistry,
    scratch: &std::path::Path,
) -> (ToyDomain<MemoryStores>, u64) {
    let domain = harness::<MemoryStores>();
    let magic = u64::from(domain.genesis().network_magic());
    let plan = plan_at_boundary(&domain, 1, Default::default());

    registry::publish(
        registry::Publishing::new(&opened(registry, scratch)),
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &Observer::silent(),
    )
    .unwrap();

    (domain, magic)
}

/// The repository opened as `dolos bootstrap stelae` opens it.
fn opened(registry: &FaultRegistry, scratch: &std::path::Path) -> Registry {
    install_crypto_provider();

    registry::open(
        &registry.repository(REPOSITORY),
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
    }
}

fn restore_into(
    source: &Registry,
    blank: &Blank<MemoryStores>,
    storage: &std::path::Path,
    magic: u64,
    observer: &Observer,
) -> Result<restore::Summary, Error> {
    registry::restore_registry(
        source,
        Point::Latest,
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
        &opened(&registry, scratch.path()),
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

    let source = Registry::open(
        &registry.repository(REPOSITORY),
        Options {
            insecure: true,
            scratch_dir: Some(scratch.path().to_path_buf()),
            read_delay: std::time::Duration::from_millis(1),
            ..Options::default()
        },
    )
    .unwrap();

    let blank = Blank::<MemoryStores>::open();
    let storage = tempfile::tempdir().unwrap();

    let error = restore_into(&source, &blank, storage.path(), magic, &Observer::silent())
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
