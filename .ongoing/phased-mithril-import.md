# Phased Mithril Import Plan

## Goal
Split Mithril bootstrap into two passes (state, archive), skip WAL during bootstrap, and make archive import fast and independent of state.

## Work Completed
- Split Mithril bootstrap into modules:
  - `src/bin/dolos/bootstrap/mithril/mod.rs` (facade: CLI args + run)
  - `src/bin/dolos/bootstrap/mithril/helpers.rs` (snapshot download + starting point helpers)
  - `src/bin/dolos/bootstrap/mithril/state.rs` (state pass)
  - `src/bin/dolos/bootstrap/mithril/archive.rs` (archive pass)
- Added state-only import in core:
  - `crates/core/src/facade.rs` now exposes `import_blocks_state_only` with state-only batch execution helpers (no archive commit).
- Updated slot tag collection to accept resolved inputs:
  - `crates/cardano/src/roll/txs.rs` signature is now `collect_slot_tags_from_block(block, resolved_inputs, tags)`.
  - Uses resolved inputs to index input-derived tags (addresses, assets, datums).
- Archive pass uses an in-memory UTxO cache (no disk cache):
  - Insert outputs into cache (decoded once).
  - Build tags using resolved inputs from cache (skip missing inputs).
  - Apply to archive.
  - Remove consumed inputs from cache.
- Archive pass no longer resolves inputs from archive on-demand.
- State pass still uses `import_blocks_state_only`.
- CLI flags `--state-only` and `--archive-only` remain in the facade.

## Key Decisions
- WAL is skipped entirely during bootstrap.
- Archive pass uses only tx logs/tags with in-memory UTxO cache.
- Missing inputs during archive pass are skipped (no error).
- No cache size limits yet (memory-heavy acceptable for now).

## Files Touched
- `src/bin/dolos/bootstrap/mithril/mod.rs`
- `src/bin/dolos/bootstrap/mithril/helpers.rs`
- `src/bin/dolos/bootstrap/mithril/state.rs`
- `src/bin/dolos/bootstrap/mithril/archive.rs`
- `crates/core/src/facade.rs`
- `crates/cardano/src/roll/txs.rs`

## Notes
- Compile checks ran successfully after refactor.
- UTxO cache is archive-only and not reused in helpers.
- `helpers.rs` is the preferred naming over `common.rs`.
