# Export preparation

A publication at sequence `P` used to be built whole from stores standing at
`P`. That ties every publication to a full index traversal and to publishing
before the replay advances. This contract separates the work:

1. **Prepare** at each boundary: capture what a later store cannot reproduce
   (`dolos_snapshot::prepare`). No index traversal, no predecessor, no history.
2. **Stage** once for a batch: `dolos_snapshot::staging` partitions the closed
   windows of every prepared publication from one frozen archive view.
3. **Finalize** in sequence: assemble each publication from its bundle, the
   staged runs and the closed windows of the final store, when the stele it
   really follows is committed.

`export::export`, and every path through it (directory and registry publication,
digest, reproduction), is the batch of one: see
[Batch of one](#batch-of-one). A publication has exactly the canonical content
the old exporter gave it at the same point, predecessor and retained list. No
stored schema, layer format, published byte or protocol rule changes.

The sections below cover the facade, then boundary preparation, then index
staging, which is the engine both paths share.

## Facade

`SnapshotSource`, implemented by the snapshot a replay workspace or session
lends, is the host's surface:

```rust
fn prepare_boundary(&self, store: &prepare::Store, plan: &Plan,
    request: &prepare::Request<'_>, observer: &Observer) -> Result<Bundle, Error>;
fn stage_batch(&self, root: &Path, bundles: &[Bundle],
    previous: &dyn Predecessor, budget: Budget) -> Result<Staged, Error>;
fn finalize_document(&self, finalizing: &Finalizing<'_>,
    previous: &dyn Predecessor) -> Result<Document, Error>;
fn finalize_repository(&self, publishing: Publishing<'_>,
    finalizing: &Finalizing<'_>, observer: &Observer) -> Result<Published, Error>;
```

A batch runs:

```rust
// While replay stands at each boundary, before it advances:
let delta = session.boundary_index_delta().cloned();  // the block's index entries
let bundle = session.snapshot().prepare_boundary(&store, &plan, &prepare::Request {
    store_contract, mutable_slots, tail: IndexTail::Delta(&delta.unwrap()),
}, &observer)?;

// Once, at the batch's frontier, with replay stopped:
let staged = source.stage_batch(&root, &bundles, &first_predecessor, budget)?;

// In ascending sequence, each after the one before it committed:
source.finalize_repository(publishing, &Finalizing { store: &store, bundle: &bundle,
    staged: &staged, producers }, &observer)?;
store.release(bundle.sequence())?;
```

Contract tests: the `boundary_preparation` module of
`crates/snapshot/tests/export.rs`, and
`the_boundary_delta_is_the_index_tail_a_traversal_finds` in `tests/engine.rs`,
which holds the delta from a real import against a traversal.

## Boundary preparation

`prepare` runs on stores `boundary::check` accepts: on the first block of `P`,
with an archive that ends there. A window's source at finalization is decided
when the boundary is prepared:

| Layer | Window | Source |
|---|---|---|
| `state-*` tip shards | | The bundle. |
| retained dump at `P` | | The tip's payload, described a second time (`carry_again`). |
| retained dump below `P` | | The predecessor, or left out with a warning, as before. |
| `blocks` | `P` | The bundle. |
| `blocks` | closed | The final archive: closed blocks do not change. |
| `indexes` | `P`, the live-boundary tail | The bundle. |
| `indexes` | closed | The staged run. |
| `log-*` | `capture_from..=P` | The bundle. A layer it does not hold did not exist at `P`. |
| `log-*` | below `capture_from` | The final archive. |
| `entity-*` | sealed at `P` | The staged run, if it holds an entry. |
| `entity-*` | not sealed at `P` | None, whatever a later view shows. |

The predecessor is asked first for every inheritable layer, exactly as an export
asks it, so a captured payload may go unused. `is_settled` still holds a publish
to the predecessor's own sequence for `log-account-epochs`.

**Why the log windows.** `capture_from` is the lower of `P - 1` and the oldest
epoch the boundary could not seal. Below it no later boundary writes: `P - 1`'s
account rows settle at the boundary into `P + 1`, a seal writes only epochs it
just sealed, and Byron's short epochs stay unsealed for several boundaries, so
they are captured for as long.

**The index tail.** `boundary::check` proves the tail is one block, the cursor's.
`IndexTail::Delta` takes that block's `ArchiveIndexDelta`, which
`BulkReplaySession::boundary_index_delta` hands over from the import that
committed it. `layers::indexes::records_of` turns it into the records a traversal
yields. A delta for any other block is refused (`ForeignTail`). Nothing is
re-resolved from block bodies, and ordinary nodes keep no new index.

A session that did not import the block has no delta: a process that died between
the commit and the capture, for example. `IndexTail::Traverse` is the explicit
recovery. While the stores stand at the boundary a slot-bounded traversal is
exact, and it costs one whole-index scan, so it is never chosen for the host.

## Bundles

A `prepare::Store` under `root`, private to the publisher and written by one
writer at a time:

- `blobs/sha256/{digest}`: payloads, named by the digest of their compressed
  bytes, in a stele directory's layout. Two boundaries that captured the same
  layer hold one file. They are compressed at level 3, and finalization
  recompresses at the profile's pinned level, so identity is never in question.
- `bundles/{sequence}.json`: the manifest, which is also the completion marker.

The **manifest** (`BUNDLE_FORMAT`) holds:

- its **identity**: format, profile and version, media type version, network
  magic, sequence, cursor slot and hash, store contract, retained list, the plan's
  windows and `mutable_slots`;
- its **owner**, the journal the host names when it opens the store;
- `capture_from` and `oldest_unsealed`;
- its **horizon**: the oldest slot finalization reads from the source archive,
  which pruning must retain until the bundle is released. `None` means every
  window was captured;
- every **layer**: descriptor, `diffId`, blob digest and both sizes;
- an xxh3-64 **checksum** over all of the above.

**Lifecycle.** Payloads are fsynced, then their directory, then the manifest is
written to `{sequence}.json.partial`, fsynced, renamed and the directory fsynced.
On Windows the directory syncs are skipped and a crash may lose the marker. A
capture that fails leaves no marker, and rerunning it converges on the same
payloads; `Store::open` drops half-written ones. After replay advances, a bundle
is required recovery data: nothing else holds that boundary.

| At `prepare`, the store holds | Result |
|---|---|
| Nothing for this sequence | Capture. |
| A complete bundle of the same identity | That bundle; nothing is captured again. |
| A complete bundle of another identity | `SourceMismatch`; the marker stands. |
| A damaged or foreign bundle | Refused. `Store::release` is the explicit recovery from damage. |

**Loading** checks the checksum, the format, the sequence, the owner and that
every payload is present at its recorded size. Contents are checked as they are
read: a replay holds each payload to its `diffId`, size and record count before
the sink finishes, and the rebuilt descriptor to the captured one, so a damaged
payload produces no descriptor and no stele.

**Finalization** first holds the sources to the bundle: the staged runs' network,
store contract and frontier, the boundary block in the archive and the horizon.
It reads nothing boundary-sensitive from the final store. A closed window neither
staged nor inherited fails with `NotStaged` rather than dropping a layer.

**Release.** `Store::release` removes the marker first, then every payload no
remaining manifest refers to, so a crash leaves unreferenced payloads for
`Store::sweep` and never a marker over missing ones. `Store::sweep` refuses while
any manifest is unreadable, because it cannot know what that one needs. A host
releases a bundle only after the sequence is committed.

`Refusal`:

| Refusal | Meaning |
|---|---|
| `NotPrepared` | No completion marker for this sequence. |
| `ForeignOwner` | The bundle belongs to another journal. |
| `SourceMismatch` | The bundle, stores or runs come from different sources. |
| `MissingArtifact` | A payload is gone or not its recorded size. |
| `Corrupt` | A manifest or payload failed its checks. |
| `ForeignTail` | The delta is not the boundary block's. |
| `HorizonLost` | The archive no longer reaches the bundle's horizon. |
| `EmptyBatch` | `stage_batch` had no bundles. |
| `Io` | The filesystem failed. |

## Batch of one

`export::export` stands on the boundary with nothing to advance the stores, so it
captures nothing: the layer writers read the stores directly, through the same
code finalization replays bundles with. It stages the plan's windows with
`Request::live`, which makes the frontier's own clamped window stageable and takes
the entity index as the view shows it. `boundary::check` has proven the archive
ends at the cursor, so that window is exact. Runs go under `Plan::scratch`
(`Plan::with_scratch`), or the system temporary directory when unset, and are
removed afterwards.

This replaces the banded traversal, so hosts migrate:

- `export::IndexBand`, `Plan::band`, `Plan::with_band`, `planning::banded`,
  `Selection::index_band` and `PlanReport`'s `band_epochs` and `band_budget_mib`
  are gone. `export::LAYER_SINK_BYTES` keeps the sink cost.
- A host should set `Plan::with_scratch` to a volume sized for the windows it
  builds. A mainnet first publish spools tens of gigabytes.
- Staging now runs before the layer pool rather than beside it, because the
  layer count needs its presence answer. A single publication's index traversal
  therefore no longer overlaps its state walks.

## Index staging

`dolos_snapshot::staging` stages the `indexes` and `entity-{kind}` layers of
several publications from one frozen archive view. `indexes` costs one tag and
one exact traversal, and every entity kind shares one traversal, however many
epochs they cover.

### Inputs

`staging::stage(root, archive, summary, mutable_slots, request, budget)`:

- `archive` is a frozen view. Nothing may write to it during the staging.
- `request.frontier` is the last block of that view, and it must carry a hash.
  The archive's tip must be at the frontier slot, and the last block stored at
  that slot must be the frontier block. A Byron main block that shares its slot
  with the boundary block before it is therefore past a frontier at that
  boundary block.
- `request.store_contract` is the archive's compatibility identity
  (`dolos_core::compat`). `request.network_magic` names the chain.
- `request.epochs` lists, per family (`Family::Indexes` or
  `Family::Entity(kind)`), the epochs to stage. Leave out inherited epochs, and
  every epoch no prepared publication will build.
- `summary` gives every window's slots. Byron and Shelley epochs differ in
  length, so windows always come from the chain summary and never from fixed
  geometry.
- `mutable_slots` is the genesis's `3k/f` (`dolos_cardano::mutable_slots`),
  which decides when the seal writes an epoch's entity entries.
- `request.live` marks a batch of one: the only publication is the frontier
  itself, so its own clamped window can be staged and what the view holds is what
  that publication carries.

### Visibility

A staged run is valid for a publication only under the rules below. A later
view does not reproduce an earlier boundary on its own.

**Closed windows only.** A window `e` can be staged when
`epoch_start(e + 1) <= frontier.slot`. A publication at sequence `S` can use a
staged window only when `e < S`. Its own window `S` is the boundary sliver from
`epoch_start(S)` to its cursor. That sliver is the **live-boundary tail**, and
staging never produces it:

- The cursor's epoch keeps growing in every later view.
- Filtering a later view by slot takes in rows that are past the cursor at the
  same slot. Byron's main block shares the boundary block's slot.

The tail has to be captured at the boundary itself, which
[boundary preparation](#boundary-preparation) does. A live request is the one
exception: its frontier is the boundary, so its traversal sees exactly the tail.

**Index rows.** The records a closed window holds in the frontier view are the
ones a publication at any later boundary holds, under three conditions:

- every prepared boundary lies on the frontier's chain;
- tag rows are keyed by slot and never rewritten;
- exact keys are written once.

A later write of an existing exact key moves its slot, and a frozen view cannot
show that it happened. The frontier check binds the view; it does not attest
to history.

**Entity seal visibility.** An entity's first-seen slot does not decide which
publication carries it. The seal at the first boundary `B` for which
`periods::is_sealable(summary, mutable_slots, e, B)` holds writes epoch `e`'s
entries, and `staging::sealed_by` returns that boundary. A publication at `S`
carries window `e`'s entity layer only if `sealed_by(e) <= S` and the run holds
an entry. A window that is not sealed at the frontier is refused at staging,
because the view does not yet hold its entries. Byron's short epochs seal more
boundaries after their epoch than Shelley's do.

**Presence.** Whether an entity layer exists comes from the staged run's
record count, read with `Staged::records` and `Staged::publishes`. No
publication scans the entity index for presence.

**Coverage.** Records outside the requested windows are dropped by the
traversal that spans them. An epoch that was not requested has no run, and
`write_layer` refuses it (`NotStaged`), so a sparse request never turns into
published coverage. An `indexes` layer exists for every staged closed window,
even an empty one.

### Outputs

`Staged::write_layer(stele, family, epoch, sequence)` finishes one layer
through the existing `SteleWriter` sinks, codecs and order checks. It returns
`None` where that publication has no such layer. `Staged::write_layers` does
the same for a list of layers, with at most `budget.workers` sinks open at
once, and returns the results in input order. The descriptors are byte-equal
to the ones a traversal of the window alone writes, and to `export::export`'s
for the same publication.

`Staged::report()` exposes the tag, exact and entity traversals the staging
started, the records its runs hold and the run bytes it wrote. Benchmarks read
traversal counts from it. A staging of zero epochs starts no traversal, and one
of any number of epochs starts at most one tag, one exact and one entity
traversal.

### Resource budgets

`staging::Budget`:

| Field | Bounds |
|---|---|
| `run_buffer` | Bytes each run buffers before they are appended to its file. Memory is the staged windows times this. |
| `scratch` | A hard limit on the run bytes one staging writes. Crossing it fails with `ScratchExceeded`, and the families the crossing traversal stages are left unmarked: `indexes`, or every pending entity kind. |
| `workers` | Layers finished at once. Each holds one compression context (`LAYER_SINK_BYTES`) and a 64 KiB read buffer. |

While a staging runs, at most one run file is open at a time: each buffer
flush opens the file, appends to it and closes it. Run bytes are the framed
records. A family's marker is a few bytes per window on top of that.

### On-disk layout and lifecycle

Under `root`:

- `{kind}/{epoch}.run`: one run per staged window. Each record is a frame of
  a little-endian `u32` length followed by the record's canonical CBOR, and
  no frame exceeds 64 KiB.
- `{kind}.json`: the family's completion marker. It holds the identity and,
  per run, the record count, the byte count and the xxh3-64 checksum of the
  framed bytes.

A family is complete only once its marker exists. Closing a family proceeds
in this order:

1. Flush every run and `fsync` it.
2. Write `{kind}.json.partial` and `fsync` it.
3. Rename it to `{kind}.json` and `fsync` the directory. Windows cannot open a
   directory to sync it, so there a crash may lose the marker, and the family
   is staged again.

A traversal that fails, whether from a read error or the scratch limit, leaves
no marker. Its partial runs therefore cannot be consumed, and the next staging
replaces them. Families completed before the failure keep their markers.

The **identity** binds a family's runs to their source and coverage. It holds
the spool format (`SPOOL_FORMAT`), the profile name and version, the layer
kind's media type (which carries the codec version), the network magic, the
frontier's slot and hash, the store contract, and every staged window with its
slots and seal boundary. When a staging finds a marker under `root`:

- the same identity, with every run at its marked size: the family is reused
  and nothing is traversed;
- a different identity: the staging is refused (`SourceMismatch`); runs are
  never reused across sources or coverage;
- the same identity with a run missing or off its marked size: the marker is
  removed and the family is staged again;
- a marker that does not parse, or names a different number of runs than
  windows: the staging is refused (`Corrupt`).

Consumption checks every frame's length and canonical form while it streams.
At the end it checks the record count, the byte count and the checksum, before
the sink is finished. A damaged run therefore produces no descriptor.

Runs are reproducible from the archive, so they are disposable. Their owner
deletes `root` once the last publication that needs them has committed.

### Errors

`Error::Staging(Refusal)`:

| Refusal | Meaning |
|---|---|
| `UnknownKind` | An entity kind this profile has no layer for. |
| `NotAtFrontier` | The archive does not end at the frontier block. |
| `OpenWindow` | A requested window is not closed at the frontier. |
| `Unsealed` | A requested entity window is not sealed at the frontier. |
| `NotStaged` | A layer was asked for a window that was not staged. |
| `NotClosedAt` | A window that is not closed in that publication: its own tail, or later. |
| `PastFrontier` | A publication past the frontier's epoch. |
| `SourceMismatch` | `root` holds runs of another identity. |
| `ScratchExceeded` | The scratch limit was crossed. |
| `Corrupt` | A run or marker failed its framing, canonical form or marker check. |
| `Io` | The filesystem failed. |

A frontier without a hash fails with `Error::UnanchoredPoint`. Archive read
errors are terminal and surface as `Error::Archive`. Order violations surface
as the codecs' `OutOfOrder`.
