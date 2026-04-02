# Bug: Stale in-memory work buffer after rollback

## Location

- Rollback: `crates/core/src/sync.rs:67-137` — modifies state store but not in-memory chain logic
- Work buffer: `crates/cardano/src/work.rs` — `WorkBuffer` tracks `last_point_seen()`
- Next forward: `crates/cardano/src/lib.rs:255-271` — `receive_block` uses stale buffer

## Description

The `rollback()` function modifies the state store (UTxOs, cursor) and truncates the WAL/archive, but does **not** update the in-memory `CardanoLogic` work buffer. After rolling back to slot P, the work buffer still holds `Restart(slot_of_last_undone_block)`.

When the next block arrives, `WorkBuffer::receive_block()` uses `self.last_point_seen().slot()` as `prev_slot` for epoch and RUPD boundary detection. This `prev_slot` is stale — it points to an undone block, not the rollback target.

## Code

After rollback, `on_rollback` returns without touching the chain logic:

```rust
// apply.rs
fn on_rollback(&self, point: &ChainPoint) -> Result<(), WorkerError> {
    self.domain.rollback(point).or_panic()?;  // modifies state store only
    Ok(())
    // CardanoLogic's work buffer is NOT updated
}
```

Then the next block arrives:

```rust
// work.rs, WorkBuffer::receive_block()
let prev_slot = self.last_point_seen().slot();  // STALE: slot of undone block
let next_slot = block.slot();

// Epoch boundary detection uses wrong prev_slot
let boundary = pallas_extras::epoch_boundary(eras, prev_slot, next_slot);

// RUPD boundary detection uses wrong prev_slot
let rupd_boundary = pallas_extras::rupd_boundary(stability_window, eras, prev_slot, next_slot);
```

## Impact

- **Missed epoch transitions**: If the rollback crosses an epoch boundary (rollback target in epoch E-1, undone block in epoch E), the stale `prev_slot` is already in epoch E. The next block in epoch E won't trigger EWRAP/ESTART because no boundary is detected between the stale `prev_slot` (epoch E) and the new block (epoch E).

- **Missed RUPD boundaries**: Similar issue — if the RUPD boundary falls between the rollback target and the undone block's slot, it won't be re-triggered.

- **Spurious boundaries**: If the gap between the stale `prev_slot` and the new block crosses a boundary that wouldn't be crossed from the rollback target, a spurious work unit fires.

## Example from logs

```
10:20:25  roll forward point=108296425   (state committed)
10:20:37  roll forward point=108296437   (state committed)
10:20:38  rollback to 108296425, undone 108296437
10:21:06  roll forward point=108296466   (new fork)
```

After rollback, work buffer is `Restart(108296437)` instead of `Restart(108296425)`. Boundary detection between 108296437→108296466 instead of 108296425→108296466.

## Fix

After `rollback()` returns, update the chain logic's work buffer to reflect the rollback target:

```rust
fn on_rollback(&self, point: &ChainPoint) -> Result<(), WorkerError> {
    self.domain.rollback(point).or_panic()?;
    
    // Update the in-memory work buffer to the rollback target
    let mut chain = self.domain.write_chain();
    chain.reset_to(point);  // new method needed on CardanoLogic
    
    Ok(())
}
```

Where `reset_to` sets the work buffer to `Restart(point.clone())`.
