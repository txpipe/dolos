# Segment lifecycle

`COMPRESSED.md` describes the bytes of a compressed segment. This document
describes how `FlatFileStore` lives with both kinds of segment at once: how a
read finds its bytes, how appends and truncation treat a compressed segment,
how a segment moves between representations without ever losing its last
good copy, and what a restart makes of a move that was interrupted.

## One segment, one representation

A segment is stored **raw** (`NNNNNN.segment`, the concatenated block bodies)
or **compressed** (`NNNNNN.zseg`, the same logical stream in the compressed
layout). Both are addressed by the same packed `BlockLocation`s: the offset
and length name bytes of the logical stream, and the store resolves them
against whichever representation the segment currently has. Nothing in the
archive index changes when a segment changes representation.

The store learns each segment's representation from the directory when it
opens and keeps it in memory from then on. Every operation on a segment takes
that segment's lock: shared for reads and appends, exclusive for anything
that changes which file is the segment. Segment locks are taken in ascending
segment order, and the writer table after them, never before, so a
transition on one segment waits for that segment's in-flight readers and for
nothing else.

Compressed segments are read through a `ReadCache` keyed by segment number
*and generation*. The generation is bumped under the exclusive lock whenever
the file behind a number changes — a transition, a truncation, a removal —
and the cache is invalidated for that number at the same time, which is why
a reader can never be handed frames, a parsed seek table or an open handle
that belong to a file the segment no longer is.

## Appends and truncation

Raw segments append as they always have: the writer handle is opened in
append mode and each batch is written and fsynced before the index entries
that point at it are committed.

A **compressed segment that must change** first becomes raw again. An append
to it, or a truncation inside it, thaws the segment — a complete, verified
raw file at the same logical offsets, published through the transition below
— and then proceeds exactly as on a raw segment: the append lands at the
logical end, the truncation cuts at the requested offset. Truncating a
segment at or past its end changes nothing and converts nothing. Truncating
any segment **to zero** removes it in whatever representation it has, without
thawing: there is nothing to carry over.

Pruning (`delete_segments_before`) removes every file of every segment below
the threshold — both representations, staged outputs and transition records
alike — after taking each segment's exclusive lock, dropping its writer
handle and invalidating its cache entries, so the space is reclaimable the
moment the call returns. Dictionaries are not touched by pruning.

## The transition

`seal` (raw → compressed) and `thaw` (compressed → raw) are one primitive
run in opposite directions, under the segment's exclusive lock:

1. **Settle.** Re-run the recovery below for this segment, so a transition
   whose retirement step failed earlier is completed before a new one
   starts. Refuse if the segment is already in the destination
   representation.
2. **Record.** Write `NNNNNN.transition` containing `seal` or `thaw`,
   through a staged file, an fsync, a rename and a directory fsync. From
   here the directory says a transition is under way and in which direction.
3. **Stage.** Write the destination to `NNNNNN.zseg.tmp` or
   `NNNNNN.segment.tmp` in the segments directory — the same file system as
   the destination, so the publish is a rename — and fsync it. A seal cuts
   frames at the block boundaries the caller supplies (the locations the
   archive index holds for the segment) and keeps any bytes those locations
   do not cover as frames of their own, so the logical stream is preserved
   whole. A thaw decodes every frame in order.
4. **Verify.** Open the staged file the way a restart would. A seal parses
   the staged segment, resolves its dictionary through the store's own
   dictionary source, decodes every frame and compares it with the raw bytes
   it stands for. A thaw reads the staged file back and checks its length
   and content hash against what was written. Anything short of a byte-exact
   match is an error.
5. **Publish.** Rename the staged file over the destination name and fsync
   the directory. The rename is the commit point: the destination is now the
   segment, in memory and on disk.
6. **Retire.** Unlink the source representation, fsync, unlink the record,
   fsync. Bump the generation and invalidate the cache.

A failure before step 5 removes the staged file and the record and returns
the error; the source representation was never touched, so a full disk, a
dictionary that is not installed, or a corrupt frame found during
verification all leave the last good copy in place. A failure after the
rename — the directory fsync of step 5, or anything in step 6 — returns the
error too, but the destination stays authoritative — the store has already
switched to it, and the record on disk says so to the next open.

Sealing needs the dictionary it compresses with to be resolvable by the store
(installed under `dictionaries/` by default), because the verification is a
real read. That is deliberate: a segment that can be written but not read
back on this machine is not published.

## Restart authority

The recovery that runs for every segment at open — and again at the start of
each transition — decides authority from the record and the representation
files together, never from file presence alone, and leaves the directory in
the at-rest shape for the representation it chose.

| record | files present                     | authority  | what recovery does                            |
|--------|-----------------------------------|------------|-----------------------------------------------|
| none   | one representation                | that one   | remove any staged file                        |
| none   | none                              | absent     | remove any staged file                        |
| none   | raw **and** compressed            | —          | **refuse to open**: nothing says which is right |
| `seal` | raw only (staged file or not)     | raw        | abort: remove staged file, remove record      |
| `seal` | compressed (raw present or not)   | compressed | finish: remove raw, remove record             |
| `seal` | neither representation            | —          | **refuse to open**: the segment is lost       |
| `thaw` | compressed only (staged or not)   | compressed | abort: remove staged file, remove record      |
| `thaw` | raw (compressed present or not)   | raw        | finish: remove compressed, remove record      |
| `thaw` | neither representation            | —          | **refuse to open**: the segment is lost       |

A staged transition record (`NNNNNN.transition.tmp`) is never a record; it is
removed and the row for "no record" applies.

Read against the six steps above, for a seal (a thaw is the mirror image with
the roles of the two files exchanged):

| interrupted after | on disk                        | restart authority |
|-------------------|--------------------------------|-------------------|
| nothing           | raw                            | raw               |
| record staged     | raw, `transition.tmp`          | raw               |
| record written    | raw, record                    | raw               |
| output staging    | raw, record, partial `.tmp`    | raw               |
| output fsynced    | raw, record, complete `.tmp`   | raw               |
| verification      | raw, record, complete `.tmp`   | raw               |
| **publish**       | raw, record, compressed        | **compressed**    |
| source unlinked   | record, compressed             | compressed        |
| record unlinked   | compressed                     | compressed        |

The destination file appears on disk only through the rename in the publish
step, after verification, which is what makes its presence beside a record
the commit point. Two consequences matter for correctness:

- A staged output is **never** trusted, however complete it looks; recovery
  removes it and the transition is simply run again. Transitions are
  idempotent for that reason.
- Once a thaw has published, the raw file is the segment, and whatever
  happens to it before the compressed file is retired — an append, a
  truncation, a crash in between — stands. A restart that still finds the
  compressed file finishes retiring it; it does not read it, and it does not
  resurrect bytes the raw file no longer has.

## Opening a store

Opening performs the recovery above for every segment, then parses each
compressed segment's metadata frame and seek table and resolves the
dictionary it names. A segment whose metadata version this build does not
read, whose dictionary is not installed, or whose installed dictionary does
not hash to the identity the segment names fails the open with an error that
names the segment and, where there is one, the dictionary. Frame payloads
are not decoded at open; a corrupt frame is a read error on the block that
touches it, as it is for a raw segment whose bytes are damaged.

Dictionaries live in `dictionaries/` under the segments directory, one file
per identity named by its SHA-256 (`<64 hex digits>.dict`), and are verified
against that name whenever they are loaded. Installing one is writing that
file; the store never deletes or rewrites one.
