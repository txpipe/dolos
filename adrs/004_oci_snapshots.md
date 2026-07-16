# ADR 004 - Deterministic OCI Snapshots

## Status

Proposed

## Context

- Dolos snapshots are currently a gzip tarball of the raw `archive/`, `state/` and `index/` database directories, uploaded to publicly accessible storage (Cloudflare R2) and addressed by a URL template (`https://dolos-snapshots.txpipe.cloud/${VERSION}/${NETWORK}/${VARIANT}/${POINT}.tar.gz`). There is no manifest, no checksum and no signature; the only integrity check is that gzip/tar fail on corrupt data.
- Every bootstrap downloads the full tarball and every publish uploads everything. There is no incremental path in either direction.
- The payload is implementation-specific: redb and fjall database files. Any change to the storage engines or their schemas breaks compatibility of every published snapshot.
- Snapshot generation is non-deterministic. The current `dolos data export` already writes deterministic tar headers, but the database bytes underneath are not reproducible: redb uses copy-on-write page allocation and fjall is an LSM tree whose on-disk segments depend on flush and compaction timing. Two nodes with identical logical content produce different bytes, so snapshots cannot be independently rebuilt, hashed or co-signed.
- By contrast, the Mithril bootstrap path already demonstrates the desired trust model (manifest + certificate verification) but requires hours of block replay; the Dolos snapshot exists precisely to avoid that replay.

The goal is a snapshot protocol and data format that:

1. supports delta uploads/downloads instead of full snapshots,
2. uses well-known data formats, agnostic of the Dolos implementation,
3. includes computed indexes and state so that restore is a mostly linear, append-only process,
4. is deterministic, so the same chain point produces byte-identical artifacts that can be hashed and signed by independent parties.

## Decision

- Use an OCI repository as the storage backend, one repository per network, targeting any OCI Distribution v1.1 registry (GHCR initially). Tags: `epoch-E` per published epoch boundary, plus a moving `latest`.
- Split chain history into epochs; each epoch produces immutable, content-addressed layers. Per epoch there are three content types, each its own layer: raw block data (`blocks`), computed archive index records (`indexes`) and epoch-boundary ledger logs (`logs`).
- Keep the ledger state as a set of "tip" layers that are swapped as a whole on every publish: 16 uniform key-value shard layers, where the UTxO set is just another key-value namespace alongside the 14 entity namespaces.
- No layer kind is mandatory for consumers: the descriptor declares what the snapshot contains, and the client selects which layers to fetch and which data to source elsewhere — block data especially, which may come from `blocks` layers, from a Mithril aggregator, or from relay replay. `sync.max_history`-driven partial fetches are one instance of this general rule, not a special mode.
- Ship an optional per-snapshot `digests` layer that pins the sha256 of every Cardano immutable-DB file (`.chunk`/`.primary`/`.secondary` — Mithril Cardano DB v2's certification granularity) covered by the snapshot. It carries no restorable data; it makes externally sourced block data verifiable against the signed descriptor, and is the enabler for a future Mithril-sourced restore mode.
- Serialize all layer content as deterministic CBOR sequences (RFC 8742 framing, RFC 8949 §4.2.1 deterministic encoding) of canonical logical records — never database files.
- Ship index data pre-hashed: the xxh3-64 key-hashing scheme used by the index stores is promoted into the format specification, so index layers are exported by direct iteration and restored by direct append, with no recomputation on either side.
- Anchor determinism and signing on a canonical descriptor (the OCI config blob, RFC 8785 canonical JSON) that lists the *uncompressed* digest of every layer plus the chain point. Independent parties reproduce and sign the descriptor's sha256; compression (pinned zstd) is transport only. Signatures are attached as OCI referrer artifacts.
- Publish only at epoch boundaries, produced by a node syncing with `stop_epoch`; restored nodes catch up the partial current epoch through regular chain-sync.
- Keep the legacy tarball path working during the transition, selected by URL scheme (`oci://` vs `https://`/`--file`), and deprecate it once the new path is proven.

## Rationale

- **OCI registries give incremental transfer for free.** Registries are content-addressed: pushing skips blobs the registry already has (HEAD by digest) and pulling fetches only the layers missing locally. Since epoch layers are immutable, a publish uploads only the newly closed epoch plus the state tip, and a restore that already holds epochs fetches only what it lacks. The referrers API provides a standard, tooling-compatible home for detached signatures. Registry infrastructure (auth, CDN distribution, garbage collection, mirroring) is commodity.
- **Epoch is the natural chunk boundary, confirmed by the codebase.** The archive flat-file store already buckets by `SLOTS_PER_SEGMENT = 432_000` (one post-Byron epoch); the apply pipeline's `WorkBuffer` never lets a work batch span an epoch; ESTART is the only cursor-advancing phase. Epoch-boundary chain points are therefore canonical, crash-safe cut points that all parties agree on — a Schelling point for reproducibility. Sizes fit registry constraints comfortably: ~580 mainnet epochs × 3 layers ≈ ~1,700 manifest descriptors (well under the 4 MiB manifest guidance) and 0.5–1.5 GB of raw blocks per epoch (well under the ~10 GB layer limit). Byron's short epochs simply make tiny layers.
- **Only logical content is deterministic, so the format serializes logical records.** Blocks are raw wire CBOR verbatim; entities are their stored minicbor values verbatim; ordering, integer widths and framing are pinned by the spec. CBOR was chosen because the payloads are already CBOR (Cardano-native), `minicbor` is already a workspace dependency, and the files remain inspectable with generic CBOR tooling — satisfying "well-known and implementation-agnostic" without inventing a container format.
- **Restore is linear by construction.** Blocks and logs append into the archive store; index records arrive pre-sorted for bulk ingestion; state records arrive sorted by key for optimal LSM ingestion; cursors are written last. The only non-append work is rebuilding the live-UTxO index dimensions from the restored UTxO set, which is a linear pass over data already in hand.
- **Pre-hashed index keys remove the most expensive pipeline stage.** The on-disk index stores keep only xxh3-64 hashes of tag keys, so logical keys are unrecoverable from disk. The initial design recomputed logical tags from raw blocks at publish time, but that requires resolving historical transaction inputs (the spent-output data lives in earlier blocks), an expensive lookup pipeline. Promoting the hash scheme into the spec eliminates recomputation on both sides, shrinks records from 30–60-byte logical keys to 8-byte hashes, and remains implementation-agnostic because xxh3-64 is a documented, widely implemented algorithm — unlike database pages, any implementation can produce and consume these records. Dimension names stay as logical strings (a small closed set), keeping layers inspectable. Shipping index layers at all is load-bearing: recomputing them at restore time is impossible without replaying the UTxO set.
- **Epoch-boundary logs must be shipped, not derived.** Reward and stake logs (`LeaderRewardLog`, `MemberRewardLog`, `PoolDepositRefundLog`, `StakeLog`) are products of ledger computation; deriving them requires full state replay, which defeats the purpose of a snapshot.
- **Determinism is anchored on uncompressed bytes.** zstd output is only stable for a pinned library version and parameters, so OCI blob digests (over compressed bytes) cannot be the cross-party identity. The descriptor lists uncompressed digests (analogous to OCI diffIDs) and is itself canonical JSON; its sha256 is the thing independent parties reproduce and sign. Compression parameters are pinned so blobs also dedupe across publishers in practice, but correctness never depends on it.
- **Mithril v2 certifies exactly the objects the digests layer pins.** Mithril's Cardano DB v2 signs a merkle tree with one leaf per individual immutable file — sha256 over raw file bytes — so a pinned digest is checkable both against our signed descriptor and, via merkle proof, against a stake-based certificate. Pinning content digests (rather than referencing aggregator URLs) keeps verification self-contained: aggregator retention is operational policy, not a protocol guarantee, and if the aggregator disappears, trust in externally sourced blocks degrades from stake-certified to publisher-attested instead of restore breaking. Immutable files are byte-identical on every honest node, so the layer is as deterministic as everything else the descriptor covers.
- **Uniform key-value state future-proofs the format.** Treating the UTxO set as namespace `utxos` means the format has one state record shape, and Dolos's planned internal refactor to fold UTxOs into the entity system (#1042) becomes invisible to the format. Sharding by the first nibble of the key balances well because keys are hash-derived (tx hashes, credentials), enables parallel fetch, and keeps every layer far from registry size limits as state grows.

## Limitations

- **Snapshots exist only at epoch boundaries.** A restored node must chain-sync the partial current epoch from a relay — up to ~5 days of blocks on mainnet (minutes to a few hours of sync). Mid-epoch state-only tip refreshes are a possible follow-up using the same format.
- **The state tip does not delta.** Every publish re-uploads the full state shards (~several GB on mainnet); every restore downloads them. Content-identical shards dedupe by digest, but reward distribution at each boundary touches most account entities, so in practice the tip is re-transferred. This matches the status quo (full snapshot per bootstrap) and only affects the tip, not history. Content-defined chunking of the state stream is a possible v2 optimization.
- **The index hash scheme becomes a compatibility surface.** Changing the xxh3-64 scheme, bucket semantics or dimension set requires a new media-type version. Old epochs can be backfilled by recomputing index layers from the (permanently available) blocks layers, so the migration path exists, but it is a real cost.
- **Determinism depends on deterministic entity encoding.** Entity minicbor values are copied verbatim, so any map-ordering or shard-merge nondeterminism in ledger code would break cross-party digests. This requires a one-time audit and is permanently enforced by an independent-builds digest comparison in CI.
- **Registry trust is not consensus trust.** Signatures prove that named parties attest to the descriptor; they do not provide Mithril-style stake-based certification. The two mechanisms remain complementary bootstrap options.
- **Stake-level verification of block data depends on Mithril artifacts.** The digests layer pins content, but checking those digests against a stake-based certificate requires a Mithril aggregator (or a mirror of its digest route and certificate chain) for the merkle proofs. Without one — and on networks without Mithril, where the layer is simply absent — trust in block data rests on descriptor signatures alone.

## Performance Impact

- **Publish**: export is a sequential scan of local stores plus zstd compression — no input resolution, no replay. Steady-state publishes upload one epoch of layers (tens to hundreds of MB compressed) plus the state shards.
- **Restore**: dominated by download and sequential ingestion. Sorted state records make fjall ingestion near-optimal; per-epoch layers allow parallel fetch and per-epoch resume, so interrupted restores lose at most one epoch of work. Light nodes (`max_history`-limited) skip historical layers entirely and download only recent epochs plus the state tip.
- **Verification** adds a streaming sha256 over each blob (compressed and uncompressed), which is negligible against network I/O.
- Rebuilding live-UTxO index dimensions at restore adds one linear pass over the UTxO set (CPU-bound CBOR parsing), overlapping with I/O.

## Alternatives Considered

1. **Keep tarballs, add a manifest + chunk-level dedup (restic/casync-style CDC over the tar stream)**
   - Pros: minimal format work; generic dedup.
   - Cons: payload remains non-deterministic DB files, so signing and implementation-agnosticism are unachievable; dedup over nondeterministic bytes is poor; still a bespoke protocol with custom tooling.

2. **Plain object storage (R2/S3) with a custom manifest of epoch files**
   - Pros: keeps current infrastructure; simpler than a registry.
   - Cons: reinvents what OCI already standardizes (content addressing, manifests, auth, signature attachment, resumable blob fetch, mirroring, GC); no ecosystem tooling.

3. **e2store/era1-style TLV container (Ethereum precedent) instead of CBOR sequences**
   - Pros: proven prior art for deterministic epoch-chunked block archives; cheap record skipping.
   - Cons: record skipping is useless here (selection happens at layer granularity; restore reads every record); introduces a second framing standard alongside the CBOR that all payloads already use; less inspectable in the Cardano ecosystem.

4. **Parquet for state/index layers**
   - Pros: columnar, widely supported by analytics tooling.
   - Cons: byte-level determinism across writer implementations/versions is not guaranteed; row-group/encoding choices are implementation-defined; poor fit for opaque CBOR blobs; heavy dependency.

5. **Logical index keys with publish-time recomputation** (the initial draft of this design)
   - Pros: format carries full-fidelity logical keys; index hash scheme stays an implementation detail.
   - Cons: publishing requires resolving historical transaction inputs from earlier blocks (expensive lookup machinery — the most complex component of the whole pipeline); records are 4–8× larger; restore must re-hash every record. Rejected in favor of pinning the hash scheme in the spec, since the recompute path survives as a one-off migration tool anyway.

6. **Mithril-style aggregator with stake-based certification**
   - Pros: strongest trust model.
   - Cons: already exists as a separate bootstrap path; does not cover Dolos' computed state/indexes; heavy infrastructure. The OCI snapshot deliberately targets a different point on the speed/trust curve, and its determinism makes multi-party attestation possible without an aggregator.

7. **State as one monolithic layer / separate special-cased UTxO layers** (earlier draft)
   - Pros: slightly simpler descriptor.
   - Cons: single layers hit registry size limits as state grows and serialize downloads; special-casing UTxOs couples the format to a Dolos internal that is already slated to change (#1042). Rejected in favor of 16 uniform key-value shards.

8. **Adopt Mithril's immutable-file format as the `blocks` layer** (chunk files stored verbatim as blobs, reusing Mithril's stake-based signatures directly)
   - Pros: per-file diffIds would literally equal the stake-certified digests — signature reuse with zero re-derivation, and blob-level dedupe with any other chunk-file mirror.
   - Cons: one layer per immutable file blows the manifest budget (~20k layer descriptors on mainnet vs ~600 epoch layers); grouping files into per-epoch archives restores the budget but destroys the byte-identity that made their signatures reusable; and `.primary`/`.secondary` are ouroboros-consensus internals — re-importing the implementation coupling this format exists to shed. Rejected as the primary format; the digests layer preserves the verification benefit, and per-file verbatim blobs remain viable for an optional mirror artifact.

9. **Reference Mithril archives by URL from the manifest** (OCI foreign layers / the `urls` field)
   - Pros: zero block hosting for the publisher.
   - Cons: `urls` is deprecated in the OCI image spec and rejected or ignored by registries; aggregator URLs and retention are operational policy, not protocol guarantees; identity should bind to content, not location. Rejected in favor of pinning content digests and leaving the transport open — which the layer-optionality rule already permits.

## Implementation Details

### Layer formats

All layers are zstd-compressed CBOR sequences (RFC 8742). Deterministic encoding profile pinned by the spec: shortest-form integers, definite lengths only, no floats, no tags. Every layer starts with a header record:

```
[format_version = 1, network_magic, kind: tstr, epoch, start_slot, end_slot]
```

(state layers carry the tip epoch and shard index instead of a slot window).

Content records per kind:

| Kind | Record | Order | Restore write path |
|---|---|---|---|
| `blocks` (per epoch) | `[slot, hash: bytes(32), body: bytes]`, body = raw wire CBOR verbatim | ascending slot, stream order for same-slot (Byron EBB) | `ArchiveWriter::apply` |
| `indexes` (per epoch) | tags: `[0, dimension: tstr, key_hash: bytes(8), slot]` with `key_hash = xxh3_64(key)` BE; exact: `[1, kind: tstr, key: bytes, slot]` for block-hash/block-number/tx | sorted, deduped | new `IndexWriter::append_prehashed` |
| `logs` (per epoch) | `[ns: tstr, log_key: bytes(40), value: bytes]`, value = stored EntityValue verbatim | `(ns, log_key)` | `ArchiveWriter::write_log` |
| `state-{00..0f}` (tip, 16 shards) | `[ns: tstr, key: bytes, value: bytes]` | `(ns, key)`; shard = first nibble of `key[0]` | dispatch on ns: `utxos` → chunked `StateWriter::apply_utxoset`, else `write_entity` |
| `digests` (tip, optional) | `[immutable_number, chunk: bytes(32), primary: bytes(32), secondary: bytes(32)]`, each sha256 over the raw file bytes | ascending `immutable_number` | none — verification metadata, not written to stores |

State namespaces: the 14 entity namespaces from `dolos_cardano::model::build_schema()` (key = 32-byte `EntityKey` verbatim, value = stored minicbor verbatim) plus `utxos` (key = `tx_hash(32) ‖ output_index(4, BE)`, value = CBOR `[era: uint, body: bytes]`). The chain point lives in the descriptor, not in a layer. Live-UTxO index dimensions (`utxo::*`) are not shipped; they are rebuilt at restore via `index_delta_from_utxo_delta`.

The `digests` layer covers the immutable files fully contained in the snapshot's block range: `lastImmutable` is derived from the boundary slot and the chunk geometry observed in the chain — canonical, never dependent on aggregator state at publish time. Digest values equal Mithril Cardano DB v2's merkle leaves (hex-decoded), so any Mithril certificate whose beacon covers `lastImmutable` can verify them via the aggregator's digest route and a merkle proof. The certificate reference is deliberately *not* part of the descriptor: certificates are produced on the aggregator's cadence, so two independent publishers at the same boundary would reference different certificates — including one would break cross-publisher determinism, while the digest values themselves are byte-stable properties of the chain.

### OCI layout and descriptor

- Repository per network (e.g. `ghcr.io/txpipe/dolos-snapshots/mainnet`); tags `epoch-E` (E = newly started epoch; layers cover epochs `0..E-1`) and `latest`.
- `artifactType: application/vnd.dolos.snapshot.v1`; layer media types `application/vnd.dolos.snapshot.{blocks|indexes|logs|state|digests}.v1+zstd`; informational annotations per layer (epoch, kind, diffid, slot window, shard).
- Config blob (`application/vnd.dolos.snapshot.descriptor.v1+json`), canonical JSON per RFC 8785:

```json
{ "schema": 1,
  "network": {"magic": 764824073, "name": "mainnet"},
  "point": {"slot": 133660800, "hash": "…"},
  "epoch": 550,
  "compression": {"algo": "zstd", "level": 9},
  "stateShards": 16,
  "history": [
    {"epoch": 548, "descriptorDigest": "sha256:…"},
    {"epoch": 549, "descriptorDigest": "sha256:…"} ],
  "layers": [
    {"kind": "blocks", "epoch": 0, "startSlot": 0, "endSlot": 21599,
     "diffId": "sha256:…", "records": 21600, "uncompressedSize": 43210000},
    {"kind": "state", "shard": 0, "diffId": "sha256:…", "records": 812345, "uncompressedSize": 402653184},
    {"kind": "digests", "lastImmutable": 6187, "diffId": "sha256:…", "records": 6188, "uncompressedSize": 618800} ] }
```

`diffId` = sha256 of the uncompressed CBOR sequence. Determinism and signing are defined only over this document's sha256. Signatures are Ed25519 over the descriptor digest, pushed as OCI referrer artifacts (`application/vnd.dolos.snapshot.signature.v1`, cosign-compatible envelope where convenient). Restore verifies registry blob digests (transport integrity) and diffIds (canonical identity).

`history` embeds the digest of every previously published descriptor, so the latest signed descriptor transitively attests the entire publication history (~80 bytes per epoch, ~50 KB after 600 epochs — negligible for a config blob). This makes attestation outlive blob retention: a snapshot whose blobs have long been garbage-collected can still be verified by anyone holding a copy, because the copy carries its own descriptor (the OCI config blob) — check that descriptor's digest against the `history` of the latest signed descriptor, then the layers against its diffIds. No external trusted storage of attestations is required.

History invariant: `history` contains exactly one entry per published boundary epoch, contiguous from the network's first published epoch (pinned per network alongside the default repository) up to `epoch - 1`, in strictly ascending epoch order — no gaps, no duplicates. JCS canonicalizes object keys but preserves array order, so the ordering is normative. Verifiers reject descriptors that violate the invariant. This is a reproducibility requirement as much as a safety one: independent publishers converge on byte-identical descriptors only if the publication schedule and history encoding are canonical; an independent party reproduces the digest chain naturally by computing each boundary descriptor while replaying the chain. If the list ever outgrows the descriptor, or succinct append-only consistency proofs become a requirement, the designated evolution is an epoch-indexed Merkle Mountain Range commitment (`{root, size}`) — a schema-versioned change that can be built retroactively from the flat list.

Note: a side-effect of anchoring identity on uncompressed content digests is that snapshot *content* can be mirrored over any content-addressed transport (e.g. IPFS) — or re-compressed with a different algorithm — and still be verified against the same signed descriptor via diffIds. Consumption is stricter than verification: the restore client expects the canonical zstd blobs referenced by the OCI manifest, so re-encoded mirrors serve archival and verification, not direct restore. This is a property of the format, not a requirement of the protocol; the OCI registry remains the canonical distribution channel.

### Code layout

New crate `crates/snapshot` (`dolos-snapshot`): `spec.rs` (descriptor, canonical JSON), `frame.rs` (deterministic CBOR-seq primitives), `layers/{blocks,indexes,logs,state}.rs`, `export.rs` / `restore.rs` (generic over `dolos_core::Domain`), `digest.rs` (streaming sha256+zstd), `oci.rs` (feature `oci`, built on the `oci-client` crate). New deps: `zstd`, `serde_jcs`, `oci-client`.

Everything is built against the engine-agnostic core traits. Existing APIs used: `ArchiveStore::get_range` / `iter_logs`, `StateStore::iter_entities` / `read_cursor`, `ArchiveWriter::apply` / `write_log`, `StateWriter::write_entity` / `apply_utxoset` / `set_cursor`, `IndexStore::initialize_schema`, `index_delta_from_utxo_delta`, `seed_wal_from_state`, `CardanoConfig.stop_epoch`. Missing APIs to add (thin wrappers over existing backend internals in both redb and fjall):

1. `StateStore::iter_utxos()` — full UTxO-set iteration (export + live-UTxO index rebuild).
2. `IndexStore` iteration of archive tag/exact records by epoch range (export).
3. `IndexWriter::append_prehashed(records)` — direct insertion of pre-hashed records (restore).

### CLI and configuration

- `dolos snapshot publish [--repo oci://…] [--output-dir DIR] [--epochs N..M] [--dry-run]` — export layers; `--output-dir` writes blobs + descriptor to disk, `--repo` pushes with blob-skip and moves tags.
- `dolos snapshot digest` — compute and print the descriptor and its sha256 from local stores (what independent verifiers run and sign).
- `dolos snapshot verify | sign --key FILE | inspect`.
- `dolos bootstrap snapshot` gains source-scheme dispatch: `oci://` → new path; https template / `--file` → legacy tarball, unchanged. `--point epoch-E|latest`; existing `--continue` drives resume; `sync.max_history` bounds how much history is fetched (subsumes the old `full`/`ledger` variants).

```toml
[snapshot]
download_url = "https://…"    # legacy, kept working, deprecated in docs
source = "oci://ghcr.io/txpipe/dolos-snapshots/mainnet"  # new, takes precedence
require_signatures = 0         # k-of-n enforcement
trusted_keys = ["ed25519:…"]  # mirrors mithril genesis_key style
```

### Publisher pipeline

1. Restore the publisher node from the previous OCI snapshot (self-hosting delta pull; first run via Mithril).
2. Sync with `chain.stop_epoch = E` until `StopEpochReached` — state lands exactly on the boundary.
3. `dolos snapshot publish` — only the newly closed epoch's layers upload; fresh state shards + descriptor; tag `epoch-E`, move `latest`. On networks with a Mithril aggregator, fetch the immutable-file digest list from the aggregator's digest route, verify it against a certificate, and write the `digests` layer for the files within the boundary.
4. Determinism job: an independent runner that synced by any means runs `dolos snapshot digest` and alerts on descriptor mismatch.
5. Matching verifiers sign and push referrer signatures; clients enforce k-of-n.

Registry hygiene: keep a trailing window of `epoch-E` tags (e.g. 12); untagged state blobs are reclaimed by registry GC; epoch blobs remain referenced by later manifests. Trust evidence for reclaimed snapshots survives in the descriptor `history` of every later publish.

### Restore pipeline

1. Resolve tag → manifest → descriptor; verify digest, schema, network magic and signatures.
2. Plan which layers to consume — no kind is mandatory: ledger-only nodes skip `blocks`/`indexes`/`logs`; a future Mithril-sourced mode fetches block data from an aggregator instead of `blocks` layers, verifying each immutable file against the `digests` layer before the usual decode→append import. Plan the epoch range from `sync.max_history`; diff against the progress file (`<storage.path>/.snapshot-restore.json`, records descriptor digest + completed layer diffIds) for `--continue`. Preflight: sum the descriptor `uncompressedSize` of the planned layers and fail early if free space at `storage.path` is insufficient; derive download progress and time-remaining estimates from the compressed blob sizes of the layers that remain to be fetched — excluding layers already completed per the progress file or already present locally — so resumed and deduplicated restores report correct totals.
3. Open stores; `IndexStore::initialize_schema()`.
4. Per epoch (checkpointed): fetch + verify `blocks`/`logs` → archive appends, commit; fetch `indexes` → pre-hashed appends, commit.
5. State tip: fetch the 16 shards (parallelizable) → dispatch per namespace; `set_cursor(descriptor.point)` last so `has_existing_data()` only ever sees complete restores; commit.
6. Rebuild live-UTxO indexes: `iter_utxos()` → `index_delta_from_utxo_delta` chunks; final chunk aligns the index cursor.
7. Delete progress file; existing `seed_wal_from_state` reseeds the WAL; the daemon chain-syncs the partial current epoch.

### Development phases

1. **Format core** — `crates/snapshot` framing/spec/layer readers+writers; the three trait additions. Verified by roundtrip unit tests, golden-digest tests (fixed input → asserted sha256), write→read→write byte-identity property tests.
2. **Local export/restore e2e** — `export.rs`/`restore.rs` (including the `digests` layer writer), `publish --output-dir`, `bootstrap snapshot --source file://`. Verified by an e2e cloned from `tests/e2e/snapshot.rs`; cross-check restored stores against an `import_blocks`-built node; determinism test (two independently synced nodes → identical descriptor digests), which is where any entity-encoding nondeterminism surfaces; audit `crates/cardano/src/model/*` as needed.
3. **OCI transport** — push with blob-skip, pull missing-only, tags. Verified against a local registry (`zot`/`registry:2`) spawned by the test; delta assertions (publish E then E+1 → only new blobs upload; pre-seeded restore fetches only missing); kill-and-`--continue` resume.
4. **Publisher productization** — `digest`/`verify`/`inspect`, `stop_epoch`-driven flow, incremental detection, CI workflow. Verified by a two-runner determinism job on preview/preprod; scheduled preprod publishing before mainnet.
5. **Signatures** — Ed25519 referrers, `trusted_keys`/`require_signatures`. Verified with generated keys plus tampered-layer/descriptor negative tests.
6. **Transition** — deprecate the tarball path in docs (keep it working); per-network default `source`. Follow-ups: "refresh" mode for already-running nodes; mid-epoch state-only tip publishes; a Mithril-sourced block restore mode (aggregator download verified against the `digests` layer, reusing the range-download/resume/import machinery from `bootstrap mithril`), which would let public-network snapshots omit `blocks` layers entirely.
