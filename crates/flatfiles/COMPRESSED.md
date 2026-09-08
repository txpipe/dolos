# Compressed segment files

This document is the normative description of the compressed form of an
archive block segment: the container, its Dolos metadata frame, the limits,
and the rules for reading one. The implementation is the `compressed` module
of the crate beside this file, `dolos-flatfiles`.

A raw segment is the concatenation of block bodies; the archive index
addresses a body by its byte offset and length inside that concatenation. A
compressed segment carries **the same byte stream** cut into independently
compressed zstd frames at block boundaries. The offsets the index holds keep
their meaning: they address the *logical* stream, and the seek table maps a
logical offset to the frame that holds it. Nothing outside the segment file
changes when a segment is compressed.

## Container

The container is the [Zstandard Seekable Format v0.1.0][seekable]: a sequence
of frames followed by a seek table in a skippable frame. Dolos adds one
skippable frame of its own at the very start.

```text
offset 0        [Dolos metadata frame]   skippable, 72 bytes, listed in the seek table with decompressed size 0
                [zstd frame 0]           standard zstd frame, one block or a group of adjacent blocks
                [zstd frame 1]
                ...
                [zstd frame N-1]
end - S         [seek table frame]       skippable, S = 8 + 8·(N+1) + 9 bytes, not listed in itself
```

Every integer is little-endian, as in zstd.

Because the layout is the standard one, any seekable-format reader resolves
logical offsets through the seek table, and any zstd decoder that skips
skippable frames — the `zstd` command line among them — regenerates the whole
logical stream from the file when supplied the dictionary, if there is one.
What such readers cannot do is *discover* the dictionary: the format has no
dictionary field, so the Dolos metadata frame carries its identity and the
store resolves it. That is a Dolos convention, not a promise that third-party
seekable readers open a dictionary-backed segment on their own.

### Data frames

Each data frame is a standard zstd frame written with:

- the content size in the frame header, so the seek table can be verified
  against the frame;
- the content checksum (`XXH64` low 32 bits) in the frame footer, so a
  corrupted frame fails to decode instead of yielding wrong bytes;
- the dictionary id in the frame header when the segment uses a trained
  dictionary (zstd derives that id; raw-content dictionaries have none and
  stamp `0`).

Frames hold whole blocks. The **frame mode** in the metadata says how block
boundaries picked frame boundaries:

- **per-block**: one frame per block, in stream order;
- **chunked** with target *T*: adjacent blocks share a frame; a frame ends
  before appending the next block would push it past *T* bytes. A block
  larger than *T* gets a frame to itself, so *T* bounds the grouping and
  nothing else — it is not a maximum block size, and a reader must not assume
  a frame is at most *T* bytes.

An empty block occupies no bytes of the logical stream and no frame. A frame
of decompressed size 0 is legal in a seek table (the metadata frame is one)
and a reader never decodes it as data.

### Seek table

The seek table is the last frame, with skippable magic `0x184D2A5E`, exactly
as the seekable spec defines it:

```text
Skippable_Magic   u32   0x184D2A5E
Frame_Size        u32   8·(N+1) + 9
Entry[0]                the metadata frame: Compressed_Size = 72, Decompressed_Size = 0
Entry[1..=N]            one per data frame, in file order
Number_Of_Frames  u32   N + 1
Descriptor        u8    0 — no per-frame checksums, no reserved bits
Seekable_Magic    u32   0x8F92EAB1
```

Each entry is `Compressed_Size u32, Decompressed_Size u32`: the frame's size on
disk including its header and footer, and the bytes it decodes to. The
physical offset of a frame is the sum of the compressed sizes before it, so
the metadata frame's entry is what keeps that sum honest; its decompressed
size of 0 keeps it out of the logical stream. Dolos never writes the optional
checksum entries (the zstd frame checksum already covers each frame) but a
reader accepts a table with the checksum flag set and ignores the checksums.

### Dolos metadata frame

The first frame is skippable, magic `0x184D2A50`, with a 64-byte payload:

```text
offset  size  field
0       4     magic            "DOLZ"
4       2     version          u16, 1
6       1     mode             1 = per-block, 2 = chunked
7       1     flags            bit 0: the data frames need a dictionary
8       4     level            i32, the zstd compression level used
12      4     frame target     u32, chunked mode's T; 0 in per-block mode
16      4     zstd dict id     u32, the id zstd stamped into the frames; must be 0 when flag bit 0 is clear
20      32    dictionary id    SHA-256 of the dictionary bytes; all zero when flag bit 0 is clear
52      4     zstd version     u32, ZSTD_versionNumber() of the writer, for diagnosis
56      8     reserved         all zero
```

The **dictionary id** is the durable identity of a dictionary: the SHA-256 of
its bytes, rendered as 64 lowercase hex digits where a name is needed. It
never changes for a given dictionary, and a dictionary is never modified —
retraining produces a new dictionary with a new identity. The 32-bit zstd id
is recorded only because zstd checks it on decode; it is not an identity
(raw-content dictionaries share `0`).

## Reading a segment

1. Read the 9-byte footer; check the seekable magic; reject reserved
   descriptor bits. Compute the seek table's size from the frame count and
   the checksum flag and check it fits the file.
2. Read the seek table frame; check its magic and declared size.
3. Sum the compressed sizes. They must land exactly on the start of the seek
   table; anything else — a truncated file, trailing bytes, an inflated
   entry — is an error. Accumulate the decompressed sizes into logical
   offsets; the sum is the logical length.
4. Entry 0 must have decompressed size 0 and its bytes must parse as a Dolos
   metadata frame of a version this reader implements. A reader refuses an
   unknown version rather than guessing; version 1 is fixed at 64 bytes and a
   payload of another length is an error.
5. If the metadata flags a dictionary, obtain the dictionary with that
   identity from the store and check the SHA-256 of what came back. Refuse to
   decode without it, and refuse a dictionary with another identity.
6. To read `length` bytes at `offset`: check `offset + length` against the
   logical length before allocating anything; find the frames overlapping the
   span through the cumulative logical offsets; for each, check the content
   size in the frame header against the entry before allocating the output,
   decode with zstd (which verifies the checksum) and check the decoded size
   against the entry; copy the requested sub-spans. A span may cross any
   number of frames.

Every check above yields an `io::Error` with a message naming the frame or
size involved; no input is allowed to panic the reader.

## Limits

- Seek table entries are 32-bit, so a frame is under 4 GiB compressed and
  decompressed. A single block already fits in a 32-bit `BlockLocation`
  length; the writer refuses a frame it cannot list.
- The seek table holds up to 2³²−1 frames and is itself under 4 GiB.
- The logical and physical lengths are 64-bit.
- The reader keeps a whole decoded frame in memory while serving a span from
  it; the chunked target therefore bounds read amplification and cache
  granularity, which is why per-block is the default for point reads.

## Compatibility

- Version 1 readers read version 1 files. A future change to the payload
  bumps the version; old readers report the new version as unsupported.
- The reserved bytes are zero in version 1 and a reader rejects non-zero
  values, so they can carry new meaning only under a new version.
- The zstd version field is diagnostic: any zstd able to decode a v1.5 frame
  with a dictionary reads these files.
- Data frames use no zstd feature that requires an external reference other
  than the dictionary — no long-distance matching across frames, no
  multi-frame state. Each frame decodes on its own.

## What this crate does and does not do

`SegmentWriter` writes a segment from a stream of block slices in either
mode. `SegmentIndex` parses one; `SegmentReader` reads one segment without
caching; `ReadCache` serves bounded concurrent reads across a store's
segments, caching parsed seek tables, decoded frames, prepared dictionaries
and open file handles under explicit entry, byte and handle limits, keyed by
segment number *and generation* so a replaced file never serves stale bytes.
`DictionarySource` is how a store hands the reader the dictionary a segment
names; the codec never searches the file system for one.

The frame, index and dictionary caches each have a separate byte budget and
entry limit. Index weight includes the frame vector's allocated capacity;
dictionary weight includes both the source bytes and zstd's prepared decoder
allocation. Fixed cache bookkeeping and Arc headers are bounded by entry
counts. An object larger than its budget is used for the current read but
never retained. Zero bytes or entries disables that cache.

`inflight_reads` bounds whole operations, starting before opening a file,
parsing a seek table, preparing a dictionary or allocating output. Zero allows
one operation; it never selects unlimited concurrency. Each admitted read can
temporarily hold an index, a prepared dictionary, its output and one decoded
frame outside the retained caches. These temporary objects are bounded in
count, not by the retained byte budgets; frame and request sizes still follow
the format limits above. Returned output and index objects belong to callers.

`handles` limits all open segment files, including files held by readers
after cache eviction, invalidation or clear. A file retains its permit until
its final reader releases it; opening and decoding use no shared seek cursor.
At the limit, a miss evicts retained handles and waits for active handles to
close. Zero disables handle retention and permits one transient file, so
`CacheLimits::DISABLED` remains bounded while retaining nothing.
`CacheStats::handles` counts retained handles and `open_handles` counts all
reserved/open handles. The other byte counters report retained allocations,
and `inflight_reads` counts admitted operations.

Dictionary preparation is fallible: a matching content hash establishes
identity, but malformed trained-dictionary contents still yield an
`InvalidData` error naming the dictionary instead of panicking.

Producing compressed files from a live store, where dictionaries live, and
how `FlatFileStore` routes reads, appends and truncation across raw and
compressed segments are the store's lifecycle, described in `LIFECYCLE.md`
beside this file.

The test fixtures under `fixtures/` are the reference bytes for this
document; `fixtures/README.md` says how they were made.

[seekable]: https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md
