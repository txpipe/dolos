//! The cache's redb disk tier under a domain: what a restart keeps.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dolos_core::{CacheDiskTier, CacheEntry, CacheNamespace, CacheStore, Domain, StateStore};
use dolos_testing::{fake_genesis_delta, toy_domain::ToyDomain};

const TOTALS: CacheNamespace = CacheNamespace::immutable("totals", 1);
const TIP: CacheNamespace = CacheNamespace::tip_scoped("tip", 1, Duration::from_secs(60));

/// A fold over the domain's UTxO set: how many, and their encoded bytes.
fn fold(domain: &ToyDomain) -> Vec<u8> {
    let (mut count, mut bytes) = (0u64, 0u64);

    for entry in domain.state().iter_utxos().unwrap() {
        let (_, utxo) = entry.unwrap();
        count += 1;
        bytes += utxo.1.len() as u64;
    }

    [count.to_be_bytes(), bytes.to_be_bytes()].concat()
}

/// A domain over the same chain each time, its cache kept in `dir`.
fn start(dir: &Path) -> ToyDomain {
    let domain = ToyDomain::new(Some(fake_genesis_delta(1_000_000)), None)
        .with_persistent_cache(dir)
        .unwrap();

    domain.cache().register(TOTALS).unwrap();
    domain.cache().register(TIP).unwrap();
    domain
}

/// Fills `key` with the fold, counting the fills that ran.
async fn fill(domain: &ToyDomain, namespace: &str, calls: &Arc<AtomicUsize>) -> Vec<u8> {
    let (folded, calls) = (domain.clone(), calls.clone());

    let value = domain.cache().get_or_fill(namespace, b"utxos", move || {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(CacheEntry::new(fold(&folded), 1))
    });

    value.await.unwrap().to_vec()
}

fn shut_down(domain: ToyDomain) {
    domain.cache().flush().unwrap();
}

#[tokio::test]
async fn a_persisted_immutable_entry_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));

    let domain = start(dir.path());
    let filled = fill(&domain, "totals", &calls).await;
    assert_eq!(filled, fold(&domain));
    shut_down(domain);

    let domain = start(dir.path());
    assert_eq!(domain.cache().disk().keys("totals").unwrap().len(), 1);

    let kept = fill(&domain, "totals", &calls).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(kept, fold(&domain));
}

#[tokio::test]
async fn a_tip_scoped_entry_is_never_in_the_file_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));

    let domain = start(dir.path());
    fill(&domain, "tip", &calls).await;
    assert!(domain.cache().get("tip", b"utxos").is_some());
    shut_down(domain);

    let domain = start(dir.path());
    assert!(domain.cache().disk().keys("tip").unwrap().is_empty());
    assert_eq!(domain.cache().get("tip", b"utxos"), None);

    let refilled = fill(&domain, "tip", &calls).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(refilled, fold(&domain));
}
