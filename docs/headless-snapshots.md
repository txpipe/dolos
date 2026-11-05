# Embedding Dolos snapshots

The snapshot profile can be embedded without Dolos service features. The
`headless_snapshot` example opens committed stores through the replay facade,
builds one selected plan, writes a directory stele, and reproduces it through
the public API:

```sh
cargo run --no-default-features --example headless_snapshot -- \
  ./dolos.toml ./stele
```

This facade was qualified from Dolos revision
`867b8596626b9d58246093a7d66f1f943af524eb`, the accepted headless replay
engine from PR #1327, against the pinned Stelae protocol/driver v0.2.0 source
revision `703cc34b`.

## Planning and reproduction

`ReplayWorkspace::snapshot()` returns a read-only `SnapshotSource`. The host
supplies the network magic, retained epochs and `facade::Selection`; the
library applies epoch restriction and producer sizing in the same order for
publication, digest and verification. The resulting plan can
be passed to:

- `publish_directory` with an explicit destination and progress observer;
- `preview_repository` or `publish_repository` with explicit
  `SnapshotRepository::publishing()` policy;
- `digest_document` with an explicit predecessor; or
- `verify_reproduction` with the published inscription.

An export of any kind, digests and reproductions included, stages its index under
`Plan::with_scratch`, or the system temporary directory when unset. A mainnet
first publish spools tens of gigabytes, so name a volume sized for it, and writable
by no one but the user running the export: a host sharing the system temporary
directory with other users must set `Plan::with_scratch`, as
[the export preparation contract](contracts/export-preparation.md) explains.

Every one of the operations above requires stores standing on the first block of the plan's
sequence epoch, with the archive ending there, as a sync stopped at that epoch
(`dolos sync catchup --follow --stop-epoch`, or a headless replay's `stop_epoch`) leaves them;
`Error::NotABoundary` is the boundary refusal, `Error::UnanchoredPoint` refuses
a hand-built plan whose cursor has no hash, and none of them writes a layer for
stores that fail the check. Their own input checks can fail first with other errors: a destination
that already holds a stele, or a reproduction whose sequence or network does
not match the published one. `boundary` asks the boundary question up front.
See "Only the boundary cut is published" in
[the profile](../crates/snapshot/PROFILE.md).

A host that publishes several boundaries per replay prepares each one with
`prepare_boundary`, stages the batch once with `stage_batch` and finishes each
publication with `finalize_repository`. Only `prepare_boundary` needs stores on
the boundary: the other two run on stores past it, where a boundary question
would refuse a valid batch. See
[the export preparation contract](contracts/export-preparation.md).

These operations return plans, documents, inscriptions or typed errors. They
do not print, exit, load configuration, or construct terminal components.

## Registry reads and restore

`facade::SnapshotRepository::open` accepts a repository, plaintext opt-in,
resolved `Auth`, scratch directory and transport tuning. The resulting client
supports `inspect`, `verify`, and registry restore. `facade::restore` is the
single restore entry point for both `RestoreInput::Directory` and
`RestoreInput::Repository`; it accepts explicit target stores, `Restoring`
resume/space policy, and an optional observer. `None`
selects a silent observer.

The library deliberately does not infer credentials, a scratch directory,
publication journal, or rebuild policy. An application may use Dolos's `node`
helpers to resolve its existing config, or provide its own policy. Publisher
cadence, acquisition, retries and shutdown behavior belong to the Stelae host;
see [the command migration](publisher-migration.md).

## Safe cold-start sequence

Treat restoration and initialization as separate state transitions:

1. Resolve the source and credentials before changing the destination.
2. Create an isolated storage root and open its state and archive stores.
3. Call `restore::execute`. On failure, leave the application uninitialized;
   do not fall back to genesis implicitly.
4. Close the restored stores, then open `ReplayWorkspace` on that storage root.
5. Call `workspace.start(...)`. This recovers the replay checkpoint/WAL and
   performs normal Dolos domain initialization without advancing a pending
   publication boundary.
6. Write any application initialization marker only after those steps succeed.

Directory deletion, reuse of partial stores, genesis fallback, and marker
placement are application policy. Keep them explicit. In particular, never
restore over a live publisher dataset or run two publishers against the same
writable stores or repository head.
