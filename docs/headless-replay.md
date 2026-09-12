# Embedding Dolos replay

Use `dolos` with default service features disabled when the host only needs
Cardano replay. The `headless_replay` example runs against a local immutable
block directory:

```sh
cargo run --no-default-features --example headless_replay -- \
  ./dolos.toml ./snapshot/immutable 500
```

## Contract

- `ReplayWorkspace::open(config, genesis)` opens a dataset without running
  ledger initialization, replaying pending work, or pruning history.
- `workspace.snapshot()` lends read-only Dolos profile operations:
  committed position, selected planning, directory or registry publication,
  digest reproduction and verification. No domain or writable storage handle
  escapes the view. See `docs/headless-snapshots.md` for the restore boundary.
- `workspace.start(stop_epoch)` consumes the inspection workspace and explicitly
  permits initialization and processing. Publish any pending boundary first.
- `session.import_blocks(blocks)` processes a nonempty batch of trusted immutable
  blocks. It returns `ReplayProgress::Committed` or `Boundary`, each carrying the
  committed input position. A stopping boundary includes its anchoring block.
  Resume input from that position: the rest of a submitted batch may be unprocessed.
- A boundary is terminal for that session. Further import calls return the same
  boundary without processing input. To advance, finish and start another session.
- After a replay execution error, the session rejects further input. Finish and reopen;
  errors indicating an incomplete ledger transition may require explicit repair.
  An empty batch is rejected before execution and does not invalidate the session.
- `session.finish()` consumes the handle, persists completed work and releases
  resources. It returns `Result<(), BulkReplayError>`, not storage bookkeeping.
  `session.run(operation)` performs finalization on both ordinary success and
  error returns, preserving both errors when necessary.
- `session.prune_history()` is explicit host policy. Neither replay completion
  nor finalization prunes automatically. Publish required history before pruning.

Sessions are not cloneable and do not implement `Domain`. Import requires
exclusive mutable access. Borrowing a profile view prevents advancing or
finishing the session while the view remains in use.

## Publisher ordering

```text
open workspace
  → inspect/plan/publish pending snapshot
  → finish inspection workspace
open workspace
  → start replay for next boundary
  → prune already-published history if policy permits
  → import trusted blocks to boundary
  → finish
repeat
```

The current backfill driver follows this order through its narrow `Workspace`,
`Session` and `Publish` interfaces. Source acquisition, retries, signals and OCI
publication policy remain the host's responsibilities.

## Internal implementation and limits

Dolos still uses its existing `ImportExt` execution path. Checkpoint maintenance
is private to the engine; an embedding application neither selects recovery
actions nor interprets their results. Normal node construction continues through
`DomainBuilder` and retains the existing startup integrity policy.

This is not a new atomic import or general corruption-repair mechanism.
Interrupted epoch transitions, inconsistent archives and other pre-existing
storage limitations are not repaired by this facade. Dropping a handle,
panicking or terminating the process is not equivalent to successful
`finish()`. Internal error details remain available in diagnostic source chains.
