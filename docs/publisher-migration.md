# Publisher command migration

Dolos no longer owns the Cardano publisher application. The source commands
`dolos snapshot publish` and `dolos snapshot backfill` moved to the
`stelae-publisher` executable in
[`txpipe/stelae`](https://github.com/txpipe/stelae). Stelae owns one-shot
publication, Mithril acquisition, the boundary-by-boundary replay loop,
checkpoint policy and publisher process lifecycle.

This is a source-level removal, not permission to replace a running publisher.
Already-deployed Dolos binaries keep working with their existing configuration
during rollout. Select the Stelae host explicitly only after a compatible
publisher artifact has been released and qualified; never run old and new
publishers against the same stores or repository head. The Stelae deployment
documentation defines the compatible upgrade and rollback procedure.

The supported Dolos command surface remains:

- `dolos snapshot digest` for deterministic local reproduction;
- `dolos snapshot verify [--reproduce]` for transport and store-backed checks;
- `dolos snapshot inspect` for repository metadata; and
- `dolos bootstrap stelae` for directory or OCI restoration.

Embedded hosts retain `ReplayWorkspace`, `SnapshotSource`,
`SnapshotRepository`, profile planning/encoding and restoration. Repository
publication takes explicit application policy: open a `SnapshotRepository`,
check `standing` and `preflight`, derive `publishing()` with the host's journal
and rebuild settings, then call `SnapshotSource::preview_repository` or
`publish_repository`. Dolos does not choose cadence, retries, acquisition,
shutdown behavior or what an up-to-date repository means.

The removal PR is based on Dolos `c3de53c5dc35c6d02420d60ce139890c8c8f6d92`
and the accepted Stelae packaging PR #5 at merge revision
`a3d3262b54b0bb3c1cc39c340b141c6fe1944244`. The next Stelae integration must
pin this removal PR's GitHub merge commit, not its working branch or head
commit.

## Compatibility record

| Surface | Before | After |
| --- | --- | --- |
| `dolos snapshot` | `publish`, `backfill`, `digest`, `verify`, `inspect` | `digest`, `verify`, `inspect` |
| root `mithril` feature | Mithril bootstrap plus `dolos-snapshot/backfill` | Mithril bootstrap only |
| `dolos-snapshot` | optional backfill dependencies and publisher policy module | profile/driver-level encode, repository, restore and reproduction APIs |
| production processes | existing Dolos publisher binaries | unchanged until an explicit Stelae rollout |

Binary sizes were measured with `cargo build --release --locked --bin dolos`
on the same machine and source tree: 56,813,584 bytes before the removal and
56,187,888 bytes after it (625,696 bytes, or 1.10%, smaller). Size is evidence,
not an acceptance threshold.
