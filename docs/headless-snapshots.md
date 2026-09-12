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
library applies epoch restriction, index banding and producer sizing in the
same order for publication, digest and verification. The resulting plan can
be passed to:

- `publish_directory` with an explicit destination and progress observer;
- `preview` or `publish` with an opened `publisher::Publisher`;
- `digest_document` with an explicit predecessor; or
- `verify_reproduction` with the published inscription.

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

The library deliberately does not infer credentials or a scratch directory.
An application may use Dolos's `node` helpers to resolve its existing config,
or provide its own policy. `publisher::Publisher::open_explicit` similarly
accepts the resolved repository, credentials, scratch and journal paths.

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
