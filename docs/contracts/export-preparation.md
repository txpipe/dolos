# Export preparation: index staging

`dolos_snapshot::staging` stages the `indexes` and `entity-{kind}` layers of
several prepared publications from one frozen archive view. Each index family
costs one archive traversal, however many epochs it covers. This document is
the contract between that engine and the boundary preparation that will drive
it. Until that preparation routes `export::export` through it, the banded
exporter remains the public path, and the two produce the same layer bytes for
every closed window.

## Inputs

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

## Visibility

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

The tail has to be captured at the boundary itself.

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

## Outputs

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
of any number of epochs starts at most one traversal per family.

## Resource budgets

`staging::Budget`:

| Field | Bounds |
|---|---|
| `run_buffer` | Bytes each run buffers before they are appended to its file. Memory is the staged windows times this. |
| `scratch` | A hard limit on the run bytes one staging writes. Crossing it fails with `ScratchExceeded`, and the family being traversed is left unmarked. |
| `workers` | Layers finished at once. Each holds one compression context (`IndexBand::SINK_BYTES`) and a 64 KiB read buffer. |

While a staging runs, at most one run file is open at a time: each buffer
flush opens the file, appends to it and closes it. Run bytes are the framed
records. A family's marker is a few bytes per window on top of that.

## On-disk layout and lifecycle

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
3. Rename it to `{kind}.json` and `fsync` the directory.

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
  removed and the family is staged again.

Consumption checks every frame's length and canonical form while it streams.
At the end it checks the record count, the byte count and the checksum, before
the sink is finished. A damaged run therefore produces no descriptor.

Runs are reproducible from the archive, so they are disposable. Their owner
deletes `root` once the last publication that needs them has committed.

## Errors

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

Archive read errors are terminal and surface as `Error::Archive`. Order
violations surface as the codecs' `OutOfOrder`.

## Not covered here

Capturing the live-boundary tail, the boundary-sensitive state, log and block
layers, prepared-bundle durability, and routing `export::export` through
staging belong to boundary preparation. This engine changes no stored schema,
layer format or published byte.
