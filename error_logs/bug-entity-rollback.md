# Bug: Entities not saved back during rollback

## Location

`crates/core/src/sync.rs:85-100` in the `rollback()` method of `SyncExt`

## Description

During a rollback, entity state (accounts, pools, epoch state, pparams) is loaded from the state store and undone in memory, but **never written back** to the state store. Only UTxO deltas and the cursor are committed.

## Code

```rust
// Lines 85-100: entities loaded and undone in memory
let entities = log.delta.iter().map(|delta| delta.key()).collect::<Vec<_>>();
let mut entities = crate::state::load_entity_chunk::<Self>(entities.as_slice(), self.state())?;

for (key, entity) in entities.iter_mut() {
    for delta in log.delta.iter_mut() {
        if delta.key() == *key {
            delta.undo(entity);
        }
    }
}
// ^^^ entities are modified but NEVER saved through the writer

// Line 107: only UTxO undo is applied to the writer
writer.apply_utxoset(&undo_data.utxo_delta)?;

// Line 129: commit only has UTxO changes + cursor
writer.commit()?;
```

Compare to the forward path in `crates/cardano/src/roll/batch.rs:269-293` which saves both entities and UTxOs:

```rust
let writer = domain.state().start_writer()?;

// Entities ARE saved
for (key, entity) in self.entities.iter_mut() {
    let NsKey(ns, key) = key;
    writer.save_entity_typed(ns, key, entity.as_ref())?;
}

// UTxOs also saved
for block in self.blocks.iter() {
    if let Some(utxo_delta) = &block.utxo_delta {
        writer.apply_utxoset(utxo_delta)?;
    }
}

writer.commit()?;
```

## Impact

After a rollback, entity state in the store still reflects the **undone** blocks. This means account balances, pool registrations, epoch state, and protocol parameters are out of sync with the actual chain position (the UTxO set and cursor).

This is a silent corruption — subsequent blocks apply their entity deltas on top of the wrong base state, compounding the error over time.

## Reproduction

Only triggered during live sync at the chain tip when the upstream relay sends a `RollBackward` event (slot battle / micro-fork). Never triggered during Mithril bootstrap (no rollbacks).

## Fix

Save the undone entities through the writer before committing:

```rust
for (key, entity) in entities.iter_mut() {
    for delta in log.delta.iter_mut() {
        if delta.key() == *key {
            delta.undo(entity);
        }
    }
}

// ADD: save undone entities back to state
for (key, entity) in entities.iter() {
    let NsKey(ns, key) = key;
    writer.save_entity_typed(ns, key, entity.as_ref())?;
}
```
