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
    previous: &dyn Predecessor, budget: Budget,
    observer: &Observer) -> Result<Staged, Error>;
fn finalize_document(&self, finalizing: &Finalizing<'_>,
    previous: &dyn Predecessor) -> Result<Document, Error>;
fn finalize_repository(&self, publishing: Publishing<'_>,
    finalizing: &Finalizing<'_>, observer: &Observer) -> Result<Published, Error>;
```

A batch runs:

```rust
// While replay stands at each boundary, before it advances:
let delta = session.boundary_index_delta().cloned();  // the block's index entries
let tail = match &delta {
    Some(delta) => IndexTail::Delta(delta),
    // A process restarted on this boundary: see "The index tail".
    None => IndexTail::Traverse,
};
let bundle = session.snapshot().prepare_boundary(&store, &plan, &prepare::Request {
    store_contract, mutable_slots, tail,
}, &observer)?;

// Once, at the batch's frontier. Replay stays stopped until the last commit:
let staged = source.stage_batch(&root, &bundles, &first_predecessor, budget, &observer)?;

// In ascending sequence, each after the one before it committed:
source.finalize_repository(publishing, &Finalizing { store: &store, bundle: &bundle,
    staged: &staged, producers }, &observer)?;
store.release(bundle.sequence())?;
```

Contract tests: the `boundary_preparation` module of
`crates/snapshot/tests/export.rs`,
`the_boundary_delta_is_the_index_tail_a_traversal_finds` in `tests/engine.rs`,
which holds the delta from a real import against a traversal, and
`tests/boundary_preparation.rs`, which holds a batch through the real ledger
writers against replays stopped at each boundary.

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
epoch the boundary could not seal. Below it nothing writes again. `P`'s window
grows with its epoch and its closing boundary. `P - 1` takes the reward update's
pool rows in the middle of `P`, and its account and pool rows settle at the
boundary into `P + 1`. A seal writes only the epochs it just sealed, and Byron's
short epochs stay unsealed for several boundaries, so they are captured for as
long.

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
- its **capture id**: random, new on every capture, and kept by a bundle `prepare`
  finds held. Staged runs name the captures they were staged for (see
  [Restarting a batch](#on-disk-layout-and-lifecycle));
- `capture_from` and `oldest_unsealed`;
- its **horizon**: the slot of the last block at or before the first slot of the
  plan's oldest closed window, or of the chain's first block when it has none that
  early. Pruning must retain that block and everything after it until the bundle is
  released, and an archive that starts exactly there is enough. Staging and
  finalization both read closed windows from the source archive, and a captured log
  window does not change that: its blocks and index rows still come from there, and
  a window below `capture_from` reads its logs from there too. Those log rows key at
  the epoch's first slot, which usually holds no block, and `prune_history` cuts them
  by slot like the blocks. So the window's first block would not do: a cut right at
  it keeps every block and loses the logs, and the layers would just be missing. A
  block at or before the first slot shows the cut did not pass it. Where the horizon
  is the chain's first block, `prune_history` takes it whenever it takes anything.
  The price is that a cut after the horizon is refused even where it lost nothing,
  because a block cannot show that; pruning to the recorded value never makes one.
  `None` means the plan covers no closed window, and then only the boundary block
  itself has to stay, so that finalization can check the chain.
  This bounds what a batch reads, not what a later `prepare` needs: that one still
  takes a whole archive (`boundary::check`), so a host prunes only after the batch's
  last `prepare`;
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
| A complete bundle of the same identity, and the stores produce its layers | That bundle. Nothing is written. |
| A complete bundle of the same identity, and the stores produce something else | `SourceMismatch` saying what differs: the first differing layer, the layer count, or how far the boundary sealed; the marker stands. |
| A complete bundle of another identity | `SourceMismatch` naming what differs; the marker stands. |
| A bundle of another build's records (profile, profile version or media type version) | `ForeignBuild`; the marker stands. |
| A damaged or foreign bundle | Refused. `Store::release` is the explicit recovery from damage. |

**Why a held bundle is walked.** Equal identity does not prove equal stores:
identity leaves out the build's ledger logic, `custom_utxos`, genesis contents and
`force_protocol`, and stores rebuilt to the same cursor can hold other rows under
any of them. So `prepare` runs the capture again into nothing and compares
descriptors, which costs what the capture cost, and a `Traverse` tail costs its
scan again. The honest path reaches this only when a process died between the
marker and advancing replay.

**Rebuilding stores.** That walk needs stores standing at the boundary. Nothing
can compare a bundle with stores rebuilt *past* it, and finalization would publish
the old capture beside the new archive. A host that rebuilds its stores releases
every unfinished bundle first, and deletes any staging root of that batch, whose
runs came from the old archive. Should it forget the root, `stage_batch` refuses
the boundaries it captures again (`staging::Refusal::SourceMismatch`).
`staging::stage` takes no captures: it refuses a root `stage_batch` wrote the same
way, whose identity holds them, and reuses the runs under a root it wrote itself,
for the same request. A host that keeps its bundles, or a `Staged` it already holds,
through a rebuild gets no refusal: they still agree with each other, and nothing can
see the stores moved.

**Loading** checks the checksum, the format, the sequence, the owner, that the
bundle was captured as the records this build publishes (profile, profile version
and media type version), and that every payload is present at its recorded size.
A later build can read an old bundle's bytes and label them as its own, and no
layer header would stop it, so that is `ForeignBuild`. Contents are checked as
they are read: a replay holds each payload to its `diffId`, size and record count
before the sink finishes, so a damaged payload produces no layer and no stele.

**Staging a batch** first holds each bundle to these stores: its store contract
against this build's current one, its network and `mutable_slots` against the
batch's, and its horizon against the archive. The traits expose no store's own
record, so a store still on an older contract this build reads is refused too,
which is the safe direction.

**Finalization** first holds the sources to the bundle: the staged runs' network,
store contract and frontier, the boundary block in the archive and the horizon.
It reads nothing boundary-sensitive from the final store. A closed window neither
staged nor forecast by the predecessor fails with `NotStaged` rather than dropping
a layer.

**A window the predecessor declines.** Staging leaves a window out when
`Predecessor::carried_forward` says the predecessor carries it, because that is
the only question it can ask without arranging anything: `adopt` puts the blob in
the transport. `carried_forward` can say yes where `adopt` later says no, for a
recorded layer whose blob the repository has since dropped. Finalization finds that
out, stages those windows for itself from the same frozen view under the staging
root, and removes them afterwards. It costs a traversal per family that needs one.

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
| `ForeignBuild` | The bundle holds another build's records. `Store::release` drops it; it takes stores at that boundary to capture it again. |
| `MissingArtifact` | A payload is gone, at loading or when read, or not its recorded size at loading. One cut short after loading reads as `Corrupt`. |
| `Corrupt` | A manifest or payload failed its checks. `Store::release` is the recovery, as it is for `MissingArtifact` and, at `prepare`, `SourceMismatch`. `Io` never calls for it. A bundle of another `BUNDLE_FORMAT` is `Corrupt` too; releasing it drops what the build that wrote it could read. |
| `ForeignTail` | The delta is not the boundary block's. |
| `HorizonLost` | The archive's first block is past the bundle's horizon. |
| `EmptyBatch` | `stage_batch` had no bundles. |
| `Io` | The machine failed, while reading a payload included. The bundle may be intact. |

## Batch of one

`export::export` stands on the boundary with nothing to advance the stores, so it
captures nothing: the layer writers read the stores directly, through the same
code finalization replays bundles with. It stages the plan's windows with
`Request::live`, which makes the frontier's own clamped window stageable and takes
the entity index as the view shows it. `boundary::check` has proven the archive
ends at the cursor, so that window is exact.

**Every export spools,** digests and reproductions included: they keep nothing,
but their runs need somewhere to be while the walk lasts. Runs go under
`Plan::scratch` (`Plan::with_scratch`), or the system temporary directory when
unset, which logs a warning. Where the spool cannot be claimed the refusal is
`staging::Refusal::Scratch`, naming the place. Running out of space later is
`staging::Refusal::Io`.

**The spool.** Each export claims a directory `dolos-staging-*` under that scratch
and holds a lock on a file inside it, and removes it when it finishes. A killed
export cannot, so the next one sweeps first: it removes every such directory whose
lock nobody holds. The system releases a lock however its holder died, so a live
peer's spool is never taken, and a digest and a publish can share a scratch
directory. A filesystem that cannot lock gets no sweep. A sweep never waits on what
it finds, so a stray file cannot hang an export: a lock that is not a plain file is
left in place, and logged.

**The scratch must not be writable by anyone but the user running the export.** A
sweep checks no owner and removes what it finds without bounding the walk, so another
user planting a deep tree there can abort the export. A host sharing the system
temporary directory with other users must set `Plan::with_scratch`.

**Progress.** Staging has no layer to announce and its records are reported once,
when their layer is finished. So it tells the observer it is alive with
`Event::Records(0)`, before each traversal and every million records across them.
At `info` it logs each traversal, the running count at each of those million-record
beats, and its final `Report`. The count runs across the traversals, so it can
include records of those before the one it names. An observer printing only
`LayerStarted` sees nothing from it until the layers begin, so the log is where an
operator tells a live traversal from a wedged one.

A long gap is not by itself a wedge. The store scans a whole dimension and filters
by slot, so a narrow window of a long chain yields records slowly: staging one
mainnet epoch took 410 s over 2.4 million records, about one beat every three
minutes (one exploratory pair, Apple M4 with an external SSD). A traversal yielding
under a million records logs at most one running count between its start and the
finish, and none if it is the staging's first, however long its scan. Judge a silence
against the gaps before it, and a first one against that rate. A killed traversal
loses its spool.

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

`staging::stage(root, archive, summary, mutable_slots, request, budget)`, or
`staging::stage_watched` with an observer for the beats described under
[Batch of one](#batch-of-one):

- `archive` is a frozen view. Nothing may write to it during the staging.
- `request.frontier` is the last block of that view, and it must carry a hash.
  The archive's tip must be at the frontier slot, and the last block stored at
  that slot must be the frontier block. A Byron main block that shares its slot
  with the boundary block before it is therefore past a frontier at that
  boundary block.
- `request.store_contract` is the archive's compatibility identity
  (`dolos_core::compat`). `request.network_magic` names the chain. `stage_batch`
  fills it from this build's current contract, never from a bundle.
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
frontier's slot and hash, the store contract, every staged window with its
slots and seal boundary, and, for a batch, the captures it was staged for. None of
it fingerprints the archive's rows, so stores rebuilt to the same frontier look the
same: what changes is the captures. When a staging finds a marker under `root`:

- the same identity, with every run at its marked size: the family is reused
  and nothing is traversed;
- a different identity: the staging is refused (`SourceMismatch`);
- the same identity with a run missing or off its marked size: the marker is
  removed and the family is staged again;
- a marker that does not parse, or names a different number of runs than
  windows: the staging is refused (`Corrupt`).

Consumption checks every frame's length and canonical form while it streams.
At the end it checks the record count, the byte count and the checksum, before
the sink is finished. A damaged run therefore produces no descriptor.

**Restarting a batch.** `stage_batch` wants fewer windows as layers are recorded
and steles commit, and has fewer bundles as they are released. Where a family's
marker is of the same source, holds every window it wants and was staged for every
one of its bundles, the runs are reused whole; the extra windows go unused, because
a publication asks its predecessor first. Where the marker leaves out a wanted window
the refusal is `Uncovered`, so a host can tell a batch that wants more from runs of
another source, frontier or capture, which stay `SourceMismatch`. A boundary captured
again, under a rebuild for example, is another capture even where it holds the same
layers, because the rows a rebuild moved may be in closed windows no bundle holds. A
bundle new to the batch is treated the same. Either way, delete the root and stage
again. `staging::stage` itself takes exact coverage, as above.

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
| `SourceMismatch` | `root` holds runs of another identity, or runs not staged for one of a batch's captures. Delete `root` once sure it is this batch's. |
| `Uncovered` | `stage_batch` wants a window the runs under `root` leave out. Runs are disposable: delete `root`. |
| `Scratch` | An export could not claim a spool under its scratch directory. |
| `ScratchExceeded` | The scratch limit was crossed. |
| `Corrupt` | A run or marker failed its framing, canonical form or marker check. |
| `Io` | The filesystem failed. |

A frontier without a hash fails with `Error::UnanchoredPoint`. Archive read
errors are terminal and surface as `Error::Archive`. Order violations surface
as the codecs' `OutOfOrder`.
