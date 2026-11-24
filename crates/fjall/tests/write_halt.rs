//! Fjall's L0 write halt on a keyspace tuned like Dolos's index keyspaces.

use std::{sync::mpsc, thread, time::Duration};

use dolos_fjall::Preset;
use fjall::AbstractTree;

/// A keyspace at the 30-run L0 write halt, with its workers idle, must still
/// accept a write: one more write is what lets the halt clear.
#[test]
fn a_write_at_the_l0_halt_returns_with_idle_workers() {
    let preset = Preset::Standard.archive();
    let dir = tempfile::tempdir().unwrap();
    let db = preset.database.builder(dir.path()).open().unwrap();
    let ks = db.keyspace("tags", || preset.tags.options()).unwrap();

    for i in 0..30u32 {
        // Keys at both ends make every table overlap, so each flush is its
        // own L0 run; a manual flush queues no compaction.
        ks.insert("a", i.to_be_bytes()).unwrap();
        ks.insert("z", i.to_be_bytes()).unwrap();
        ks.tree.flush_active_memtable(0).unwrap();
    }
    assert_eq!(30, ks.tree.l0_run_count());

    let (tx, rx) = mpsc::channel();
    let writer = ks.clone();
    thread::spawn(move || tx.send(writer.insert("final", "value")).unwrap());
    rx.recv_timeout(Duration::from_secs(10))
        .expect("the write stayed halted at 30 L0 runs with idle workers")
        .unwrap();
}
