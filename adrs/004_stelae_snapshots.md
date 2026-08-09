# ADR 004 - Stelae: Deterministic OCI Snapshots

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

None of those four goals is Cardano-specific, and neither are the mechanisms that satisfy them: framing, content-addressed delta transfer, a canonical signable document and multi-party attestation are generic. Only the *payload* is Dolos-specific — which layer kinds exist, what a record contains, that the publication sequence is an epoch. Naming the whole thing "dolos snapshot" would therefore bake a product name into a general mechanism, leave third parties no collision-free way to publish their own data with it, and make a later extraction a rename of every identifier already in production. This ADR names the mechanism **Stelae** and specifies it as a protocol with a Dolos **profile**.

## Decision

- Name the mechanism **Stelae** (*stele*: a standing inscribed slab; one stele = one published artifact set at one sequence point). Specify it as a generic protocol parameterized by a **profile**; Dolos ships the first profile, `io.txpipe.dolos.cardano`. Stelae owns framing, the signable document, transport, attestation and restore planning; a profile owns layer kinds, record shapes, position, parameters, tag rendering and store semantics. The two ship as two crates with a build-checkable dependency boundary.
- Namespace by vendor so profiles coexist: payload media types are `application/vnd.{vendor}.stele.{kind}.v{n}+{codec}` with a vendor slot the publisher controls (`dolos` for ours); `application/vnd.stelae.*` is reserved for the protocol's own envelope types and never used for payloads. Clients fail closed on an unknown profile or an unimplemented profile major version.
- Use an OCI repository as the storage backend, one repository per (profile, network), targeting any OCI Distribution v1.1 registry (GHCR initially). Tags: an immutable tag per sequence, rendered by the profile (`epoch-E` for Dolos), plus a moving `latest`.
- Split chain history into epochs; each epoch produces immutable, content-addressed layers. Per epoch there are three content types, each its own layer: raw block data (`blocks`), computed archive index records (`indexes`) and epoch-boundary ledger logs (`logs`).
- Keep the ledger state as a set of "tip" layers that are swapped as a whole on every publish: 16 uniform key-value shard layers, where the UTxO set is just another key-value namespace alongside the 14 entity namespaces.
- No layer kind is mandatory for consumers: the inscription declares what the stele contains, and the client selects which layers to fetch and which data to source elsewhere — block data especially, which may come from `blocks` layers, from a Mithril aggregator, or from relay replay. `sync.max_history`-driven partial fetches are one instance of this general rule, not a special mode.
- Ship an optional per-stele `digests` layer that pins the sha256 of every Cardano immutable-DB file (`.chunk`/`.primary`/`.secondary` — Mithril Cardano DB v2's certification granularity) covered by the snapshot. It carries no restorable data; it makes externally sourced block data verifiable against the signed inscription, and is the enabler for a future Mithril-sourced restore mode.
- Serialize all layer content as deterministic CBOR sequences (RFC 8742 framing, RFC 8949 §4.2.1 deterministic encoding) of canonical logical records — never database files.
- Ship index data pre-hashed: the xxh3-64 key-hashing scheme used by the index stores is promoted into the profile specification, so index layers are exported by direct iteration and restored by direct append, with no recomputation on either side.
- Anchor determinism and signing on the **inscription** — the OCI config blob, RFC 8785 canonical JSON — which lists the *uncompressed* digest of every layer plus the profile, sequence and position. Independent parties reproduce and sign the inscription's sha256; compression (pinned zstd) is transport only. Signatures are attached as OCI referrer artifacts. ("Inscription" rather than "descriptor": OCI already uses *descriptor* for the `{mediaType,digest,size}` objects inside a manifest.)
- Publish only at epoch boundaries, produced by a node syncing with `stop_epoch`; restored nodes catch up the partial current epoch through regular chain-sync.
- Keep the legacy tarball path working during the transition, selected by URL scheme (`oci://` vs `https://`/`--file`), and deprecate it once the new path is proven. Dolos's user-facing vocabulary stays "snapshot" (`dolos snapshot …`, `[snapshot]`, `bootstrap snapshot`); "stele" is the protocol's word for the same artifact.

## Rationale

- **A named, profile-parameterized protocol costs nothing now and buys portability later.** Everything except the payload is reusable by any project shipping large reproducible datasets, and the protocol is specified before a single line is written, so the boundary can be enforced by the build (`cargo tree -p stelae` free of `dolos-*`) rather than discovered during a future extraction. Vendor-namespaced media types plus a fail-closed profile field mean a third-party publisher can put their steles in the same registry — even the same namespace — without colliding with ours or forking the spec, and generic tooling (list, verify, sign) works across all of them because the envelope types are shared.
- **OCI registries give incremental transfer for free.** Registries are content-addressed: pushing skips blobs the registry already has (HEAD by digest) and pulling fetches only the layers missing locally. Since epoch layers are immutable, a publish uploads only the newly closed epoch plus the state tip, and a restore that already holds epochs fetches only what it lacks. The referrers API provides a standard, tooling-compatible home for detached signatures. Registry infrastructure (auth, CDN distribution, garbage collection, mirroring) is commodity.
- **Epoch is the natural chunk boundary, confirmed by the codebase.** The archive flat-file store already buckets by `SLOTS_PER_SEGMENT = 432_000` (one post-Byron epoch); the apply pipeline's `WorkBuffer` never lets a work batch span an epoch; ESTART is the only cursor-advancing phase. Epoch-boundary chain points are therefore canonical, crash-safe cut points that all parties agree on — a Schelling point for reproducibility. Sizes fit registry constraints comfortably: ~580 mainnet epochs × 3 layers ≈ ~1,700 manifest descriptors (well under the 4 MiB manifest guidance) and 0.5–1.5 GB of raw blocks per epoch (well under the ~10 GB layer limit). Byron's short epochs simply make tiny layers.
- **Only logical content is deterministic, so the format serializes logical records.** Blocks are raw wire CBOR verbatim; entities are their stored minicbor values verbatim; ordering, integer widths and framing are pinned by the spec. CBOR was chosen because the payloads are already CBOR (Cardano-native), `minicbor` is already a workspace dependency, and the files remain inspectable with generic CBOR tooling — satisfying "well-known and implementation-agnostic" without inventing a container format.
- **Restore is linear by construction.** Blocks and logs append into the archive store; index records arrive pre-sorted for bulk ingestion; state records arrive sorted by key for optimal LSM ingestion; cursors are written last. The only non-append work is rebuilding the live-UTxO index dimensions from the restored UTxO set, which is a linear pass over data already in hand.
- **Pre-hashed index keys remove the most expensive pipeline stage.** The on-disk index stores keep only xxh3-64 hashes of tag keys, so logical keys are unrecoverable from disk. The initial design recomputed logical tags from raw blocks at publish time, but that requires resolving historical transaction inputs (the spent-output data lives in earlier blocks), an expensive lookup pipeline. Promoting the hash scheme into the profile eliminates recomputation on both sides, shrinks records from 30–60-byte logical keys to 8-byte hashes, and remains implementation-agnostic because xxh3-64 is a documented, widely implemented algorithm — unlike database pages, any implementation can produce and consume these records. Dimension names stay as logical strings (a small closed set), keeping layers inspectable. Shipping index layers at all is load-bearing: recomputing them at restore time is impossible without replaying the UTxO set.
- **Epoch-boundary logs must be shipped, not derived.** Reward and stake logs (`LeaderRewardLog`, `MemberRewardLog`, `PoolDepositRefundLog`, `StakeLog`) are products of ledger computation; deriving them requires full state replay, which defeats the purpose of a snapshot.
- **Determinism is anchored on uncompressed bytes.** zstd output is only stable for a pinned library version and parameters, so OCI blob digests (over compressed bytes) cannot be the cross-party identity. The inscription lists uncompressed digests (analogous to OCI diffIDs) and is itself canonical JSON; its sha256 is the thing independent parties reproduce and sign. Compression parameters are pinned so blobs also dedupe across publishers in practice, but correctness never depends on it.
- **Mithril v2 certifies exactly the objects the digests layer pins.** Mithril's Cardano DB v2 signs a merkle tree with one leaf per individual immutable file — sha256 over raw file bytes — so a pinned digest is checkable both against our signed inscription and, via merkle proof, against a stake-based certificate. Pinning content digests (rather than referencing aggregator URLs) keeps verification self-contained: aggregator retention is operational policy, not a protocol guarantee, and if the aggregator disappears, trust in externally sourced blocks degrades from stake-certified to publisher-attested instead of restore breaking. Immutable files are byte-identical on every honest node, so the layer is as deterministic as everything else the inscription covers.
- **Uniform key-value state future-proofs the format.** Treating the UTxO set as namespace `utxos` means the format has one state record shape, and Dolos's planned internal refactor to fold UTxOs into the entity system (#1042) becomes invisible to the format. Sharding by the first nibble of the key balances well because keys are hash-derived (tx hashes, credentials), enables parallel fetch, and keeps every layer far from registry size limits as state grows.

## Limitations

- **Snapshots exist only at epoch boundaries.** A restored node must chain-sync the partial current epoch from a relay — up to ~5 days of blocks on mainnet (minutes to a few hours of sync). Mid-epoch state-only tip refreshes are a possible follow-up using the same format.
- **The state tip does not delta.** Every publish re-uploads the full state shards (~several GB on mainnet); every restore downloads them. Content-identical shards dedupe by digest, but reward distribution at each boundary touches most account entities, so in practice the tip is re-transferred. This matches the status quo (full snapshot per bootstrap) and only affects the tip, not history. Content-defined chunking of the state stream is a possible v2 optimization.
- **The index hash scheme becomes a compatibility surface.** Changing the xxh3-64 scheme, bucket semantics or dimension set requires a new media-type version. Old epochs can be backfilled by recomputing index layers from the (permanently available) blocks layers, so the migration path exists, but it is a real cost.
- **Profile evolution is a second compatibility surface.** The profile major version and the payload media-type version move together, independently of the protocol version, and clients reject profile majors they do not implement. That is the price of the extension point: three version axes (protocol schema, profile, media type) must be kept coherent, and the conformance suite is what keeps them honest.
- **Determinism depends on deterministic entity encoding.** Entity minicbor values are copied verbatim, so any map-ordering or shard-merge nondeterminism in ledger code would break cross-party digests. This requires a one-time audit and is permanently enforced by an independent-builds digest comparison in CI.
- **Registry trust is not consensus trust.** Signatures prove that named parties attest to the inscription; they do not provide Mithril-style stake-based certification. The two mechanisms remain complementary bootstrap options.
- **Stake-level verification of block data depends on Mithril artifacts.** The digests layer pins content, but checking those digests against a stake-based certificate requires a Mithril aggregator (or a mirror of its digest route and certificate chain) for the merkle proofs. Without one — and on networks without Mithril, where the layer is simply absent — trust in block data rests on inscription signatures alone.

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
   - Cons: already exists as a separate bootstrap path; does not cover Dolos' computed state/indexes; heavy infrastructure. Stelae deliberately targets a different point on the speed/trust curve, and its determinism makes multi-party attestation possible without an aggregator.

7. **State as one monolithic layer / separate special-cased UTxO layers** (earlier draft)
   - Pros: slightly simpler inscription.
   - Cons: single layers hit registry size limits as state grows and serialize downloads; special-casing UTxOs couples the format to a Dolos internal that is already slated to change (#1042). Rejected in favor of 16 uniform key-value shards.

8. **Adopt Mithril's immutable-file format as the `blocks` layer** (chunk files stored verbatim as blobs, reusing Mithril's stake-based signatures directly)
   - Pros: per-file diffIds would literally equal the stake-certified digests — signature reuse with zero re-derivation, and blob-level dedupe with any other chunk-file mirror.
   - Cons: one layer per immutable file blows the manifest budget (~20k layer descriptors on mainnet vs ~600 epoch layers); grouping files into per-epoch archives restores the budget but destroys the byte-identity that made their signatures reusable; and `.primary`/`.secondary` are ouroboros-consensus internals — re-importing the implementation coupling this format exists to shed. Rejected as the primary format; the digests layer preserves the verification benefit, and per-file verbatim blobs remain viable for an optional mirror artifact.

9. **Reference Mithril archives by URL from the manifest** (OCI foreign layers / the `urls` field)
   - Pros: zero block hosting for the publisher.
   - Cons: `urls` is deprecated in the OCI image spec and rejected or ignored by registries; aggregator URLs and retention are operational policy, not protocol guarantees; identity should bind to content, not location. Rejected in favor of pinning content digests and leaving the transport open — which the layer-optionality rule already permits.

10. **Specify the mechanism as a Dolos feature, without a protocol name or profile boundary** (the shape of this ADR before the Stelae amendment)
    - Pros: one crate, one vocabulary, no extension machinery to design or test.
    - Cons: a third-party publisher has no collision-free namespace and would have to fork the spec; the Dolos context absorbs decisions (framing, attestation, transport) unrelated to a data node; and extraction later means renaming every media type, tag and identifier already published. The boundary costs one crate and one CI check today and is irreversible-cheap only before implementation starts.

## Implementation Details

### Naming, profiles and media types

Envelope types are protocol-owned and shared by every profile; payload types are vendor-owned:

| Role | Media type | Owner |
|---|---|---|
| Artifact type (manifest) | `application/vnd.stelae.stele.v1` | protocol |
| Config blob (the inscription) | `application/vnd.stelae.inscription.v1+json` | protocol |
| Signature (referrer artifact) | `application/vnd.stelae.signature.v1` | protocol |
| Layer payloads | `application/vnd.{vendor}.stele.{kind}.v{n}+{codec}` | vendor |
| — Dolos profile | `application/vnd.dolos.stele.{blocks\|indexes\|logs\|state\|digests}.v1+zstd` | Dolos |

Normative rules for coexistence:

1. Payload media types must carry a vendor slot the publisher controls. `vnd.stelae.*` is reserved for envelope types and is never a payload type — Stelae defines no payload format.
2. Profile names are reverse-DNS and vendor-owned; this profile is `io.txpipe.dolos.cardano`, version 1. The short token in media types (`dolos`) follows IANA `vnd.` custom.
3. The protocol never parses layer bodies or a profile's opaque objects. An unknown profile name, or a profile major version the client does not implement, is a clean refusal — never a partial or misinterpreted restore.
4. One repository per (profile, dataset). Sharing a registry namespace is safe: discovery filters on the common `artifactType`, the `profile` field discriminates, and tags are rendered by the profile.
5. Signatures are generic and cover the inscription digest, which itself binds the profile — so signing and verification tooling is shared across vendors.

### Layer formats

All layers are zstd-compressed CBOR sequences (RFC 8742). Deterministic encoding profile pinned by the spec: shortest-form integers, definite lengths only, no floats, no tags. Every layer starts with a protocol-defined header record that makes the blob self-describing even when detached from its registry:

```
[format_version = 1, profile: tstr, kind: tstr, scope: any]
```

`scope` is opaque to the protocol. The Dolos profile encodes `[network_magic, epoch, start_slot, end_slot]` for epoch layers, `[network_magic, epoch, shard]` for state layers and `[network_magic, epoch, last_immutable]` for the digests layer.

Content records per kind (Dolos profile):

| Kind | Record | Order | Restore write path |
|---|---|---|---|
| `blocks` (per epoch) | `[slot, hash: bytes(32), body: bytes]`, body = raw wire CBOR verbatim | ascending slot, stream order for same-slot (Byron EBB) | `ArchiveWriter::apply` |
| `indexes` (per epoch) | tags: `[0, dimension: tstr, key_hash: bytes(8), slot]` with `key_hash = xxh3_64(key)` BE — except dimension `metadata`, see below; exact: `[1, kind: tstr, key: bytes, slot]` for block-hash/block-number/tx | sorted, deduped | new `IndexWriter::append_prehashed` |
| `logs` (per epoch) | `[ns: tstr, log_key: bytes(40), value: bytes]`, value = stored EntityValue verbatim | `(ns, log_key)` | `ArchiveWriter::write_log` |
| `state` (tip, 16 shards, `scope.shard` = 0..15) | `[ns: tstr, key: bytes, value: bytes]` | `(ns, key)`; shard = first nibble of `key[0]` | dispatch on ns: `utxos` → chunked `StateWriter::apply_utxoset`, else `write_entity` |
| `digests` (tip, optional) | `[immutable_number, chunk: bytes(32), primary: bytes(32), secondary: bytes(32)]`, each sha256 over the raw file bytes | ascending `immutable_number` | none — verification metadata, not written to stores |

One exception to the tag hashing rule is normative for `indexes` v1: records in dimension `metadata` carry the logical u64 metadata label **verbatim** (big-endian) in `key_hash`, never hashed. The index stores keep metadata labels as raw labels rather than hashes, and the layer ships the stored form — that is the whole point of the pre-hashed design. `parameters.indexKeyHash` therefore describes every dimension *except* `metadata`. A publisher that hashes metadata labels produces structurally valid records that restore cleanly but can never be matched by a metadata query; conformance tooling must check this dimension specifically (#1149 tracks whether a future media-type version unifies the rule).

State namespaces: the 14 entity namespaces from `dolos_cardano::model::build_schema()` (key = 32-byte `EntityKey` verbatim, value = stored minicbor verbatim) plus `utxos` (key = `tx_hash(32) ‖ output_index(4, BE)`, value = CBOR `[era: uint, body: bytes]`). The chain point lives in the inscription's `position`, not in a layer. Live-UTxO index dimensions (`utxo::*`) are not shipped; they are rebuilt at restore via `index_delta_from_utxo_delta`.

The `digests` layer covers the immutable files fully contained in the stele's block range: `lastImmutable` is derived from the boundary slot and the chunk geometry observed in the chain — canonical, never dependent on aggregator state at publish time. Digest values equal Mithril Cardano DB v2's merkle leaves (hex-decoded), so any Mithril certificate whose beacon covers `lastImmutable` can verify them via the aggregator's digest route and a merkle proof. The certificate reference is deliberately *not* part of the inscription: certificates are produced on the aggregator's cadence, so two independent publishers at the same boundary would reference different certificates — including one would break cross-publisher determinism, while the digest values themselves are byte-stable properties of the chain.

### OCI layout and the inscription

- Repository per (profile, network) — e.g. `ghcr.io/txpipe/dolos-snapshots/mainnet`; tags `epoch-E` (E = newly started epoch; layers cover epochs `0..E-1`) and `latest`. The protocol requires an immutable tag per sequence plus a moving `latest`; the profile renders the strings.
- `artifactType: application/vnd.stelae.stele.v1`; layer media types per the table above; three annotations per layer, named in "The manifest" below — one of them normative, the other two informational.
- Config blob (`application/vnd.stelae.inscription.v1+json`), canonical JSON per RFC 8785. Generic keys plus three profile-owned opaque objects — `position`, `parameters` and each layer's `scope`:

```json
{ "schema": 1,
  "profile": {"name": "io.txpipe.dolos.cardano", "version": 1},
  "sequence": 550,
  "position": { "network": {"magic": 764824073, "name": "mainnet"},
                "point": {"slot": 133660800, "hash": "…"},
                "epoch": 550 },
  "parameters": { "stateShards": 16, "indexKeyHash": "xxh3-64" },
  "compression": {"algo": "zstd", "level": 9},
  "history": [
    {"sequence": 548, "inscriptionDigest": "sha256:…"},
    {"sequence": 549, "inscriptionDigest": "sha256:…"} ],
  "layers": [
    {"kind": "blocks", "mediaType": "application/vnd.dolos.stele.blocks.v1+zstd",
     "diffId": "sha256:…", "records": 21600, "uncompressedSize": 43210000,
     "scope": {"epoch": 0, "startSlot": 0, "endSlot": 21599}},
    {"kind": "state", "mediaType": "application/vnd.dolos.stele.state.v1+zstd",
     "diffId": "sha256:…", "records": 812345, "uncompressedSize": 402653184,
     "scope": {"shard": 0}},
    {"kind": "digests", "mediaType": "application/vnd.dolos.stele.digests.v1+zstd",
     "diffId": "sha256:…", "records": 6188, "uncompressedSize": 618800,
     "scope": {"lastImmutable": 6187}} ] }
```

`sequence` is the protocol's ordering key; the Dolos profile sets it to the epoch. `diffId` = sha256 of the uncompressed CBOR sequence. `position`, `parameters` and `scope` are canonicalized by JCS like every other key, so determinism holds without the protocol interpreting them; verifiers reject unknown *generic* top-level keys, so extension happens only inside those three objects. Determinism and signing are defined only over this document's sha256. Signatures are Ed25519 over the inscription digest, pushed as OCI referrer artifacts (`application/vnd.stelae.signature.v1`, cosign-compatible envelope where convenient). Restore verifies registry blob digests (transport integrity) and diffIds (canonical identity).

`history` embeds the digest of every previously published inscription, so the latest signed inscription transitively attests the entire publication history (~80 bytes per sequence, ~50 KB after 600 epochs — negligible for a config blob). This makes attestation outlive blob retention: a stele whose blobs have long been garbage-collected can still be verified by anyone holding a copy, because the copy carries its own inscription (the OCI config blob) — check that inscription's digest against the `history` of the latest signed one, then the layers against its diffIds. No external trusted storage of attestations is required.

History invariant: `history` contains exactly one entry per published sequence, contiguous from the network's first published sequence (pinned per network alongside the default repository) up to `sequence - 1`, in strictly ascending order — no gaps, no duplicates. JCS canonicalizes object keys but preserves array order, so the ordering is normative. Verifiers reject inscriptions that violate the invariant. This is a reproducibility requirement as much as a safety one: independent publishers converge on byte-identical inscriptions only if the publication schedule and history encoding are canonical; an independent party reproduces the digest chain naturally by computing each boundary inscription while replaying the chain. If the list ever outgrows the inscription, or succinct append-only consistency proofs become a requirement, the designated evolution is a sequence-indexed Merkle Mountain Range commitment (`{root, size}`) — a schema-versioned change that can be built retroactively from the flat list.

Note: a side-effect of anchoring identity on uncompressed content digests is that layer *content* can be mirrored over any content-addressed transport (e.g. IPFS) — or re-compressed with a different algorithm — and still be verified against the same signed inscription via diffIds. Consumption is stricter than verification: the restore client expects the canonical zstd blobs referenced by the OCI manifest, so re-encoded mirrors serve archival and verification, not direct restore. This is a property of the format, not a requirement of the protocol; the OCI registry remains the canonical distribution channel.

#### The manifest

A stele in a registry is one OCI image manifest, and its shape is closed: a conforming publisher writes exactly the fields below, and a conforming client refuses anything else.

- `schemaVersion: 2`; `mediaType: application/vnd.oci.image.manifest.v1+json`; `artifactType: application/vnd.stelae.stele.v1`.
- `config` is the inscription's descriptor: `mediaType` is `application/vnd.stelae.inscription.v1+json`, `digest` is the sha256 of the canonical inscription bytes — the same digest independent parties reproduce and sign — and `size` is those bytes' length.
- `layers`: one descriptor per inscription layer, **in inscription order**. Each carries the layer's `mediaType` exactly as the inscription states it, the compressed blob's `digest` and `size`, and the three annotations below.
- No `subject` and no manifest-level `annotations`.

The manifest bytes are canonical JSON per RFC 8785, through the same canonicalizer as the inscription, and are pushed verbatim: the protocol has one answer to "what are the bytes of this JSON document", not two that agree until they do not.

The per-layer annotation keys are reverse-DNS under `stelae.store`, a domain TxPipe owns:

| Key | Status | Value |
| --- | --- | --- |
| `store.stelae.layer.diffId` | **normative** | the layer's `diffId`, exactly as the inscription states it |
| `store.stelae.layer.kind` | informational | the layer's profile-defined kind |
| `store.stelae.layer.scope` | informational | the layer's scope object as stringified canonical JSON (annotation values are strings) |

`store.stelae.layer.diffId` is the identity→blob map — the thing a registry hands over for free and a directory has to rebuild by decompressing every blob. A client that does not read it cannot fetch a layer; it is the one annotation a reader must understand. The other two exist so a human or a generic registry tool can see what a blob covers without fetching the config blob, and a client may ignore them.

#### Manifest–inscription agreement

The manifest and the inscription are two views of one stele — the inscription holds identity, the manifest holds transport — and a disagreement between them, in either direction, is a refusal, never a preference.

A publisher refuses to build a manifest — before anything is pushed — when the inscription describes a layer that was never written, or a layer was written that the inscription does not describe: a blob nothing attests must not be published.

A client refuses a manifest — before any blob is fetched — when:

- `artifactType` is missing. This fails closed by choice: a registry that strips the OCI 1.1 discovery field has published something a client cannot recognize as a stele, and reading it anyway would make the discovery contract advisory.
- `artifactType` is present and is not `application/vnd.stelae.stele.v1`.
- the config descriptor's media type is not the inscription's.
- the manifest's layer count differs from the inscription's.
- a layer carries no `store.stelae.layer.diffId` annotation, so nothing says which layer it holds.
- a layer's `diffId` annotation disagrees with the inscription's layer *at that position*. Positional correspondence is a check of its own: a manifest carrying the right blobs in the wrong order passes the map and fails the order.
- a layer's media type disagrees with the inscription's at that position.

#### The manifest size ceiling

A manifest past **4 MiB** (`stelae::MANIFEST_SIZE_LIMIT`) is refused before the push. The figure is not a limit the OCI specification imposes; it is the ceiling registries converge on, and the refusal is measured on the exact canonical bytes that would have been pushed, so it names the document and its layer count instead of arriving later as a registry's `413`.

The arithmetic is counted in layers, because layers are what the ceiling counts: a descriptor with its annotations costs ~350 bytes, so the ceiling falls near 12,000 layers. A mainnet stele today is ~1,816 layers — ~600 epochs × 3 per-epoch kinds, plus 16 state shards — a manifest of roughly 0.6 MB, about a seventh of the ceiling. (The Rationale's "~1,700 manifest descriptors" is decision-time sizing of the same artifact; this paragraph is the authoritative count, and it counts layers rather than epochs.)

#### What the transport requires of its host

- **A process that opens a registry client must have installed a process-default rustls `CryptoProvider` first.** The transport ships no crypto backend of its own (`reqwest/rustls-no-provider`): the backend the client library would otherwise pick, `aws-lc-rs`, wants `cmake` on every build machine — the dependency this workspace already goes out of its way to avoid — so it stays out of the tree and the choice of provider moves to the program. In Dolos, `main()` installs `ring`. Omitting the install is a panic when the registry client opens, not a link error.
- **Authentication is the host's decision, in one of three shapes.** The client is opened with credentials its caller supplies — anonymous, a bearer token, or an HTTP Basic pair — and never sources them itself. Which identity a program authenticates as is that program's credential policy, and where it keeps its credentials is that program's deployment: a protocol library that read an environment variable would be deciding both on its host's behalf, and naming the variable would freeze that decision into a published API. **So this specification names no environment variable and no configuration key**, and `stelae::oci::Options::auth` is the whole of the interface. Dolos's own answer is under "CLI and configuration" below.

  Anonymous remains legitimate and is what a genuinely public repository wants. It is not what a registry that authenticates every request wants, and that is the deployment Dolos is heading for: read access to a stele repository is free and identity-less, and still credentialed.

### Code layout

Two crates, both workspace members. The split is the protocol/profile boundary made mechanical: **`cargo tree -p stelae` must contain no `dolos-*` package**, checked in CI, so extracting the protocol later is a directory move rather than a refactor.

```
crates/stelae/            # package `stelae` — protocol, zero dolos deps
  lib.rs          # errors, protocol constants, envelope media types
  frame.rs        # deterministic CBOR-seq record read/write
  inscription.rs  # schema, JCS encode/verify, digest, history invariant
  profile.rs      # Profile trait, layer-kind registry, media-type & tag naming rules
  digest.rs       # streaming sha256 + zstd (diffId + blob digest in one pass)
  sign.rs         # Ed25519 detached signatures, trusted keys, k-of-n
  plan.rs         # restore planning: layer selection, progress file, resume, preflight
  oci.rs          # feature `oci`: push with blob-skip, pull missing-only, tags, referrers
  tests/toy_profile.rs   # a second, trivial profile — proves the core carries no Dolos assumption

crates/snapshot/          # package `dolos-snapshot` — the io.txpipe.dolos.cardano profile
  lib.rs          # DolosProfile: name/version, media types, tag rendering, position/parameters
  layers/{blocks,indexes,logs,state,digests}.rs
  export.rs       # stores -> layers, generic over dolos_core::Domain
  restore.rs      # layers -> store writes, per-epoch checkpointing
```

New deps: `zstd`, `serde_jcs`, `ed25519-dalek`, `oci-client`. The Dolos-side crate keeps the name `snapshot` because that is this project's word for the artifact (`dolos snapshot`, `[snapshot]`, `tests/e2e/snapshot.rs`); `stele` is the protocol's word for the same thing.

Everything is built against the engine-agnostic core traits. Existing APIs used: `ArchiveStore::get_range` / `iter_logs`, `StateStore::iter_entities` / `read_cursor`, `ArchiveWriter::apply` / `write_log`, `StateWriter::write_entity` / `apply_utxoset` / `set_cursor`, `IndexStore::initialize_schema`, `index_delta_from_utxo_delta`, `seed_wal_from_state`, `CardanoConfig.stop_epoch`. Missing APIs to add (thin wrappers over existing backend internals in both redb and fjall):

1. `StateStore::iter_utxos()` — full UTxO-set iteration (export + live-UTxO index rebuild).
2. `IndexStore` iteration of archive tag/exact records by epoch range (export).
3. `IndexWriter::append_prehashed(records)` — direct insertion of pre-hashed records (restore).

### CLI and configuration

- `dolos snapshot publish [--repo oci://…] [--output-dir DIR] [--epochs N..M] [--dry-run]` — export layers; `--output-dir` writes blobs + inscription to disk, `--repo` pushes with blob-skip and moves tags.
- `dolos snapshot digest` — compute and print the canonical inscription and its sha256 from local stores (what independent verifiers run and sign).
- `dolos snapshot verify | sign --key FILE | inspect`.
- `dolos bootstrap snapshot` gains source-scheme dispatch: `oci://` → new path; https template / `--file` → legacy tarball, unchanged. `--point epoch-E|latest`; existing `--continue` drives resume; `sync.max_history` bounds how much history is fetched (subsumes the old `full`/`ledger` variants).

```toml
[snapshot]
download_url = "https://…"    # legacy, kept working, deprecated in docs
source = "oci://ghcr.io/txpipe/dolos-snapshots/mainnet"  # new, takes precedence
require_signatures = 0         # k-of-n enforcement
trusted_keys = ["ed25519:…"]  # mirrors mithril genesis_key style

[stelae.registry]              # who this node reads the registry as
user = "…"                     # seeded by `dolos init`
# password = "…"               # optional; omitted means the official
                               # registry's, compiled into the binary
# token = "…"                   # a bearer registry instead; excludes `user`
```

The official registry's read-only password is a **published secret**: it is what makes stele distribution free and identity-less while still authenticated. It is compiled into the binary rather than seeded into the file, so `dolos init` writes a `user` and no password and a rotation reaches every node that takes a release, instead of having to be found again in every generated `dolos.toml`. A node pointing at a private registry sets `password` and gets its own.

A publisher's full-access pair is a real secret and belongs in the environment or a secret manager, never in this file. **Dolos introduces no environment variable for it**: `RootConfig` is already loaded through a `config::Environment` layer with the `DOLOS` prefix, so a publisher exports

```sh
DOLOS_STELAE_REGISTRY_USER=…
DOLOS_STELAE_REGISTRY_PASSWORD=…
```

and the override applies by the same mechanism and with the same precedence as every other setting. That is the whole of the environment story — nothing in Dolos reads a registry credential by hand, which is what keeps one answer to "where does configuration come from" rather than two. A node carrying the read-only user in `dolos.toml` therefore publishes with nothing to remove first.

Two shapes are refusals rather than precedence rules, checked once the configuration has resolved: `token` together with `user`, and `password` with no `user`. An operator who supplied two identities meant one of them, and a client that guessed would authenticate as one nobody chose — which on a registry whose credentials carry different capabilities is the difference between a publish and a 403 nobody can explain.

The resolution is `dolos::common::stele_registry_auth`, a pure function of `[stelae.registry]` that hands the answer to the transport as a value. Another host embedding `stelae` decides its own credential sources, and this specification constrains none of them.

### Publisher pipeline

1. Restore the publisher node from the previous stele (self-hosting delta pull; first run via Mithril).
2. Sync with `chain.stop_epoch = E` until `StopEpochReached` — state lands exactly on the boundary.
3. `dolos snapshot publish` — only the newly closed epoch's layers upload; fresh state shards + inscription; tag `epoch-E`, move `latest`. On networks with a Mithril aggregator, fetch the immutable-file digest list from the aggregator's digest route, verify it against a certificate, and write the `digests` layer for the files within the boundary.
4. Determinism job: an independent runner that synced by any means runs `dolos snapshot digest` and alerts on inscription mismatch.
5. Matching verifiers sign and push referrer signatures; clients enforce k-of-n.

Registry hygiene: keep a trailing window of `epoch-E` tags (e.g. 12); untagged state blobs are reclaimed by registry GC; epoch blobs remain referenced by later manifests. Trust evidence for reclaimed steles survives in the `history` of every later inscription.

### Restore pipeline

1. Resolve tag → manifest → inscription; verify its digest, schema, profile name and major version, network magic and signatures.
2. Plan which layers to consume — no kind is mandatory: ledger-only nodes skip `blocks`/`indexes`/`logs`; a future Mithril-sourced mode fetches block data from an aggregator instead of `blocks` layers, verifying each immutable file against the `digests` layer before the usual decode→append import. Plan the epoch range from `sync.max_history`; diff against the progress file (`<storage.path>/.snapshot-restore.json`, records inscription digest + completed layer diffIds) for `--continue`. Preflight: sum the `uncompressedSize` of the planned layers and fail early if free space at `storage.path` is insufficient; derive download progress and time-remaining estimates from the compressed blob sizes of the layers that remain to be fetched — excluding layers already completed per the progress file or already present locally — so resumed and deduplicated restores report correct totals.
3. Open stores; `IndexStore::initialize_schema()`.
4. Per epoch (checkpointed): fetch + verify `blocks`/`logs` → archive appends, commit; fetch `indexes` → pre-hashed appends, commit.
5. State tip: fetch the 16 shards (parallelizable) → dispatch per namespace; `set_cursor(position.point)` last so `has_existing_data()` only ever sees complete restores; commit.
6. Rebuild live-UTxO indexes: `iter_utxos()` → `index_delta_from_utxo_delta` chunks; final chunk aligns the index cursor.
7. Delete progress file; existing `seed_wal_from_state` reseeds the WAL; the daemon chain-syncs the partial current epoch.

Steps 1–2 and the fetch/verify half of steps 4–5 are protocol code; the store writes are profile code.

### Development phases

**1a. Stelae core** — `crates/stelae`: framing, inscription (schema, JCS, digest, history invariant), the `Profile` trait and naming rules, streaming digest/compression, signatures. Verified by CBOR-seq roundtrip and write→read→write byte-identity property tests, a JCS inscription golden test, history-invariant tests (gap/duplicate/out-of-order → reject), fail-closed tests (unknown generic key, unknown profile, higher profile major), a toy non-Dolos profile exercising the full path, and the `cargo tree -p stelae` boundary check.

**1b. Dolos profile core** — `crates/snapshot`: `DolosProfile`, layer readers/writers, and the three trait additions with backend impls and adapter enums. Verified by per-layer roundtrip unit tests and golden-digest tests (fixed input → asserted sha256, catching encoding drift).

**2. Local export/restore e2e** — `export.rs`/`restore.rs` (including the `digests` layer writer), `publish --output-dir`, `bootstrap snapshot --source file://`. Verified by an e2e cloned from `tests/e2e/snapshot.rs`; cross-check restored stores against an `import_blocks`-built node; determinism test (two independently synced nodes → identical inscription digests), which is where any entity-encoding nondeterminism surfaces; audit `crates/cardano/src/model/*` as needed.

**3. OCI transport** — push with blob-skip, pull missing-only, tags. Verified against a local registry (`zot`/`registry:2`) spawned by the test; delta assertions (publish E then E+1 → only new blobs upload; pre-seeded restore fetches only missing); kill-and-`--continue` resume.

**4. Publisher productization** — `digest`/`verify`/`inspect`, `stop_epoch`-driven flow, incremental detection, CI workflow. Verified by a two-runner determinism job on preview/preprod; scheduled preprod publishing before mainnet.

**5. Signatures** — Ed25519 referrers, `trusted_keys`/`require_signatures`. Verified with generated keys plus tampered-layer/inscription negative tests.

**6. Transition** — deprecate the tarball path in docs (keep it working); per-network default `source`. Follow-ups: "refresh" mode for already-running nodes; mid-epoch state-only tip publishes; a Mithril-sourced block restore mode (aggregator download verified against the `digests` layer, reusing the range-download/resume/import machinery from `bootstrap mithril`), which would let public-network snapshots omit `blocks` layers entirely.
