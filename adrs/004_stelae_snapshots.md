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
- Split chain history into epochs; each epoch produces immutable, content-addressed layers. Per epoch there are three content types: raw block data (`blocks`), computed archive index records (`indexes`) and epoch-boundary ledger logs. The first two are one layer each; the logs are **one layer per log namespace** (`log-{ns}`, six of them), so a change to one namespace's log shape costs a backfill of that namespace's blobs rather than of every log layer ever published.
- Keep the ledger state as a set of "tip" layers that are swapped as a whole on every publish: one `state-{ns}` kind per state namespace, sharded 16 ways for the four chain-scale namespaces and a single blob for the rest, where the UTxO set is just another key-value namespace alongside the 13 entity namespaces.
- No layer kind is mandatory for consumers: the inscription declares what the stele contains, and the client selects which layers to fetch and which data to source elsewhere — block data especially, which may come from `blocks` layers, from a Mithril aggregator, or from relay replay. `sync.max_history`-driven partial fetches are one instance of this general rule, not a special mode. The same holds for a kind the client has never heard of: a restore skips it and reports the skip, so adding a kind is an additive change rather than one that bricks every deployed reader. A publisher gets no such latitude — it attests every layer it lists.
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
- **The state tip does not delta.** Every publish re-uploads the full state layers (~several GB on mainnet); every restore downloads them. Content-identical layers dedupe by digest, but reward distribution at each boundary touches most account entities, so in practice the tip is re-transferred. This matches the status quo (full snapshot per bootstrap) and only affects the tip, not history. Content-defined chunking of the state stream is a possible v2 optimization.
- **State history has the granularity of the configured list, and no finer.** Retained dumps make "the state as of epoch E" recoverable for the epochs a publisher named, at the cost of one full state set per retained epoch in the registry — which is why the list stays around 10–20 rather than per-epoch. An epoch nobody retained is not recoverable from steles at all, only by replay. Attestation inherits the same granularity: a signature over an inscription attests the dumps that inscription carries, so an epoch added to the list later is attested only from the publish that first cuts or backfills it, never retroactively. **Reproduction stops at the same line.** `verify --reproduce` rebuilds the dump cut at the stele's own sequence — it is the tip, and the tip is in the stores — but a dump for a closed epoch is not in a store standing at the tip and never will be, so it is carried forward from the document under verification and taken on trust. A retained dump's provenance therefore rests on signatures rather than on independent reproduction, which is the same footing `history` has always been on. A publisher that adopts a list before backfilling publishes short dumps and a warning per (kind, epoch) until a backfill run produces them.
- **The index hash scheme becomes a compatibility surface.** Changing the xxh3-64 scheme, bucket semantics or dimension set requires a new media-type version. Old epochs can be backfilled by recomputing index layers from the (permanently available) blocks layers, so the migration path exists, but it is a real cost.
- **Profile evolution is a second compatibility surface.** The profile major version and the payload media-type version move together, independently of the protocol version, and clients reject profile majors they do not implement. That is the price of the extension point: three version axes (protocol schema, profile, media type) must be kept coherent, and the conformance suite is what keeps them honest.
- **Determinism depends on deterministic entity encoding.** Entity minicbor values are copied verbatim, so any map-ordering or shard-merge nondeterminism in ledger code would break cross-party digests. The one-time audit this called for is **done**: writing a fully-populated canary for every namespace (`crates/snapshot/tests/field_registry.rs`) reads every field of every model type a stele can carry, and the suite's repeated-construction assertion is what keeps the property from regressing. It found one real defect — `RollingStats::registered_pools` was a `HashSet`, whose per-instance iteration order made the `epochs` namespace irreproducible across publishers of identical state — now an ordered set, at `schemas.epochs` revision 2. Two encodings are deliberate and deterministic rather than defects: `StakeLog::relative_size` is an IEEE-754 `f64` (the profile's only float, inside an opaque entity value rather than in a framed record), and `RationalNumber` carries CBOR tag 30, likewise only inside entity values. The independent-builds digest comparison in CI remains the standing enforcement for what a canary cannot reach.
- **Pre-fix `epochs` rows do not self-heal in history.** A store synced before the `registered_pools` fix holds `log-epochs` rows, one per past boundary, in whatever order that node's hash container produced; they are never rewritten, so two such nodes publish different bytes for the same past epochs forever. The state tip heals at the next boundary. Making published history homogeneous is a backfill run's job, which decision 0026 assigns to the publisher pipeline, not to the format.
- **Registry trust is not consensus trust.** Signatures prove that named parties attest to the inscription; they do not provide Mithril-style stake-based certification. The two mechanisms remain complementary bootstrap options.
- **Stake-level verification of block data depends on Mithril artifacts.** The digests layer pins content, but checking those digests against a stake-based certificate requires a Mithril aggregator (or a mirror of its digest route and certificate chain) for the merkle proofs. Without one — and on networks without Mithril, where the layer is simply absent — trust in block data rests on inscription signatures alone.

## Performance Impact

- **Publish**: export is a sequential scan of local stores plus zstd compression — no input resolution, no replay. Steady-state publishes upload one epoch of layers (tens to hundreds of MB compressed) plus the state tip's layers.
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
   - Cons: single layers hit registry size limits as state grows and serialize downloads; special-casing UTxOs couples the format to a Dolos internal that is already slated to change (#1042). Rejected in favor of one key-value kind per state namespace, sharded 16 ways where the population is chain-scale.

8. **Adopt Mithril's immutable-file format as the `blocks` layer** (chunk files stored verbatim as blobs, reusing Mithril's stake-based signatures directly)
   - Pros: per-file diffIds would literally equal the stake-certified digests — signature reuse with zero re-derivation, and blob-level dedupe with any other chunk-file mirror.
   - Cons: one layer per immutable file blows the manifest budget (~20k layer descriptors on mainnet vs ~600 epoch layers); grouping files into per-epoch archives restores the budget but destroys the byte-identity that made their signatures reusable; and `.primary`/`.secondary` are ouroboros-consensus internals — re-importing the implementation coupling this format exists to shed. Rejected as the primary format; the digests layer preserves the verification benefit, and per-file verbatim blobs remain viable for an optional mirror artifact.

9. **Reference Mithril archives by URL from the manifest** (OCI foreign layers / the `urls` field)
   - Pros: zero block hosting for the publisher.
   - Cons: `urls` is deprecated in the OCI image spec and rejected or ignored by registries; aggregator URLs and retention are operational policy, not protocol guarantees; identity should bind to content, not location. Rejected in favor of pinning content digests and leaving the transport open — which the layer-optionality rule already permits.

10. **Specify the mechanism as a Dolos feature, without a protocol name or profile boundary** (the shape of this ADR before the Stelae amendment)
    - Pros: one crate, one vocabulary, no extension machinery to design or test.
    - Cons: a third-party publisher has no collision-free namespace and would have to fork the spec; the Dolos context absorbs decisions (framing, attestation, transport) unrelated to a data node; and extraction later means renaming every media type, tag and identifier already published. The boundary costs one crate today and is irreversible-cheap only before implementation starts.

## Implementation Details

### Naming, profiles and media types

Envelope types are protocol-owned and shared by every profile; payload types are vendor-owned:

| Role | Media type | Owner |
|---|---|---|
| Artifact type (manifest) | `application/vnd.stelae.stele.v1` | protocol |
| Config blob (the inscription) | `application/vnd.stelae.inscription.v1+json` | protocol |
| Signature (referrer artifact) | `application/vnd.stelae.signature.v1` | protocol |
| Layer payloads | `application/vnd.{vendor}.stele.{kind}.v{n}+{codec}` | vendor |
| — Dolos profile | `application/vnd.dolos.stele.{blocks\|indexes\|log-{ns}\|state-{ns}\|digests}.v1+zstd` | Dolos |

Normative rules for coexistence:

1. Payload media types must carry a vendor slot the publisher controls. `vnd.stelae.*` is reserved for envelope types and is never a payload type — Stelae defines no payload format.
2. Profile names are reverse-DNS and vendor-owned; this profile is `io.txpipe.dolos.cardano`, version 1. The short token in media types (`dolos`) follows IANA `vnd.` custom.
3. The protocol never parses layer bodies or a profile's opaque objects. An unknown profile name, or a profile major version the client does not implement, is a clean refusal — never a partial or misinterpreted restore. A layer whose **kind** the client does not implement is deliberately not one of those: the client cannot store what it does not model, so it skips the layer and reports the skip, and only a `required: true` in that layer's `scope` turns the skip back into a refusal naming the kind and the scope. The publish side keeps the strict rule in both cases — a publisher that cannot build a kind must not chain onto a stele carrying it, since the alternatives are dropping the layer from the repository silently or attesting bytes it never read. `required` lives in the profile-owned `scope` and not in an OCI annotation, so it is signed planning input rather than unsigned transport metadata; the protocol carries it and never reads it. It is one-way — a kind published as required forever constrains readers older than it — so marking one is an ADR-level act and rare by construction.
4. One repository per (profile, dataset). Sharing a registry namespace is safe: discovery filters on the common `artifactType`, the `profile` field discriminates, and tags are rendered by the profile.
5. Signatures are generic and cover the inscription digest, which itself binds the profile — so signing and verification tooling is shared across vendors.

Rule 3 answers for a kind the reader does not *know*. It says nothing about a kind the publisher no longer *carries*, and `required: true` cannot be stretched to cover one: `required` is a property of a layer, and a retired kind has no layer to put it on. Absence is already meaningful in this format — a `log-{ns}` layer exists if and only if it holds a record, and a restore passes over a kind it does not recognise — so a reader that still models `log-member-rewards`, finds no such layer, and reports a clean restore has just built a node with no reward history and no way to have noticed.

**A profile therefore declares the namespaces it defines, and a retirement is declared rather than inferred.** `parameters.schemas` carries an entry for every namespace the profile version defines; a namespace it has retired keeps its entry at revision `0`, which is not a schema revision and reads as "this version defines no records here". A restore compares that map against the namespaces it models, before a store is opened: an entry that is missing or zero for one it models refuses the restore and names the namespace. Only presence is judged, never the revision's value — a revision the reader has not seen describes bytes it can still parse, and gating on it would make every additive append breaking, which is exactly what the `.v{x}` contract below exists to avoid.

Like `required`, the rule binds forward and not backward: it constrains readers from the version that implements it onward, and cannot reach the ones already deployed. What protects those, for the four namespaces retired so far, is that every one of them was also a *state* namespace, and the state tip's completeness check refuses a stele missing a kind it expects. A log-only namespace would have had no such backstop, and that is the case this rule exists for. Retiring a namespace is an ADR-level act, for the same reason marking one `required` is.

Rule 3's skip is available at layer granularity and at no finer one. Index **dimensions** stay fail-closed: `indexes` is a single layer per epoch, so an unknown dimension surfaces mid-stream — record by record, inside a layer the plan has already committed to restoring — where skipping it would be silent data loss rather than a visible plan-time choice, and where the store cannot look the name up in any case (it keeps a hash of the name, not the name). Changing the dimension set therefore remains a media-type-version event. The same reasoning is why a new *namespace* is additive and a new dimension is not: a namespace arrives as its own `log-{ns}` or `state-{ns}` layer, which a plan can decline; a dimension arrives inside one.

### Layer formats

All layers are zstd-compressed CBOR sequences (RFC 8742). Deterministic encoding profile pinned by the spec: shortest-form integers, definite lengths only, no floats, no tags. Every layer starts with a protocol-defined header record that makes the blob self-describing even when detached from its registry:

```
[format_version = 1, profile: tstr, kind: tstr, scope: any]
```

`scope` is opaque to the protocol. The Dolos profile encodes `[network_magic, epoch, start_slot, end_slot]` for epoch layers, `[network_magic, epoch, shard]` for every state layer — one shape across all fourteen kinds, single-blob namespaces included, whose one layer is shard 0 — and `[network_magic, epoch, last_immutable]` for the digests layer.

The state layers carry **two roles over that one header shape**, and only the *descriptor* scope tells them apart: a tip is `{"shard": n}`, a retained dump is `{"epoch": E, "shard": n}`. The header is deliberately blind to the distinction, and that is what makes the dump a publish cuts at `sequence == E` the tip's own bytes rather than a copy of them — same header, same records, one `diffId`, one blob under two descriptors. See "State history" below.

Content records per kind (Dolos profile):

| Kind | Record | Order | Restore write path |
|---|---|---|---|
| `blocks` (per epoch) | `[slot, hash: bytes(32), body: bytes]`, body = raw wire CBOR verbatim | ascending slot, stream order for same-slot (Byron EBB) | `ArchiveWriter::apply` |
| `indexes` (per epoch) | tags: `[0, dimension: tstr, key_hash: bytes(8), slot]` with `key_hash = xxh3_64(key)` BE — except dimension `metadata`, see below; exact: `[1, kind: tstr, key: bytes, slot]` for block-hash/block-number/tx | sorted, deduped | new `IndexWriter::append_prehashed` |
| `log-{ns}` (per epoch, per log namespace, omitted when empty) | `[log_key: bytes(40), value: bytes]`, value = stored EntityValue verbatim | `log_key` | `ArchiveWriter::write_log` into the namespace the kind names |
| `state-{ns}` (tip or retained dump, per state namespace, `scope.shard` = 0..`parameters.shards[ns]`-1) | `[key: bytes, value: bytes]` | `key`; shard = first nibble of `key[0]` for a 16-way namespace, 0 for a single blob | dispatch on the kind: `state-utxos` → chunked `StateWriter::apply_utxoset`, else `write_entity` into the namespace the kind names |
| `digests` (tip, optional) | `[immutable_number, chunk: bytes(32), primary: bytes(32), secondary: bytes(32)]`, each sha256 over the raw file bytes | ascending `immutable_number` | none — verification metadata, not written to stores |

One exception to the tag hashing rule is normative for `indexes` v1: records in dimension `metadata` carry the logical u64 metadata label **verbatim** (big-endian) in `key_hash`, never hashed. The index stores keep metadata labels as raw labels rather than hashes, and the layer ships the stored form — that is the whole point of the pre-hashed design. `parameters.indexKeyHash` therefore describes every dimension *except* `metadata`. A publisher that hashes metadata labels produces structurally valid records that restore cleanly but can never be matched by a metadata query; conformance tooling must check this dimension specifically (#1149 tracks whether a future media-type version unifies the rule).

State namespaces: the 16 entity namespaces from `dolos_cardano::model::build_schema()` (key = 32-byte `EntityKey` verbatim, value = stored minicbor verbatim) plus `utxos` (key = `tx_hash(32) ‖ output_index(4, BE)`, value = CBOR `[era: uint, body: bytes]`). The chain point lives in the inscription's `position`, not in a layer. Live-UTxO index dimensions (`utxo::*`) are not shipped; they are rebuilt at restore via `index_delta_from_utxo_delta`.

State kinds: one per state namespace, and the set is closed — 14 of them, spelled `state-` followed by the namespace with `_` rewritten to `-`, by the same rule and for the same reasons as the log kinds below. The namespace is therefore **not** in the record — it is the layer — which is what puts the fail-closed edge of a breaking change on exactly the namespace that broke, and lets a reader skip a namespace this profile does not define at the transport rather than choking on one shared layer. The shard count is **specification, never tuning**: `utxos`, `accounts`, `assets` and `datums` split 16 ways, every other namespace is a single blob, and `parameters.shards` reports the map so a reader never has to discover it from the data. Re-sharding a namespace is a media-type-version event for that namespace's kind. Every shard of every kind is published, empty ones included, so tip completeness is structural: a restore requires all 14 kinds and, per kind, exactly the shards its count promises.

**State history: retained dumps at configured epochs, plus the moving tip.** A stele's state is the tip — the ledger as of `sequence`, swapped whole by every publish — and, for each epoch a publisher retains, an immutable **dump** of the state as of that epoch. The two are the same kinds, the same records and the same shard geometry; a dump differs from a tip in its descriptor scope, which names the epoch, and in nothing else.

- **The retained set is configuration, not derivation.** `snapshot.state_epochs` names it. Era boundaries are one sensible criterion and cherry-picked epochs another; which epochs are worth a dump is operational, so nothing derives the list from the chain summary. The list is strictly ascending and never names epoch 0, and it is refused where it is read rather than where a dump is cut — it reaches `parameters` before any layer is written. Publishers are expected to keep it around 10–20: per-epoch dumps were rejected outright (~46k manifest descriptors on mainnet against a ~12k ceiling).
- **The list is signed input.** It is echoed verbatim into `parameters.stateEpochs`, so two publishers of one network configured differently produce different parameters, different inscription digests, and a divergence an operator reads out of a one-field diff instead of hunting through layers for. It is therefore **normative per network** and belongs pinned wherever the default repository is pinned: a publisher quietly running a different list self-ejects from co-signing.
- **Production rule.** At the publish where `sequence` equals a retained epoch E, E's dump is cut from the tip: one walk of the store, one sink per shard, one blob, and the transport attests the result a second time under the dump's scope. Nothing is compressed twice and nothing crosses the wire twice. At a publish standing past E, the dump is adopted from the predecessor by the same scope equality every immutable layer uses. A dump for a past epoch that no predecessor carries is a **warning and a shorter stele**, never a failed publish: this publish's stores hold the tip, and the state as of an epoch it has moved past is not in them to be written. Producing one is a backfill run's job.
- **Inheritance.** The rule "no state layer is ever inherited" was about the tip role, in two independent ways, and both still hold of it: the tip changes every publish, and its scope names no epoch, so scope equality could not tell one publish's shard from another's. A dump's scope does name its epoch, so it inherits, checkpoints and resumes exactly as a `blocks` layer does — including through the resumption record an interrupted publish leaves.
- **Adoption is per shard.** A predecessor that can hand over fifteen of a dump's sixteen shards hands over fifteen. Adopting is an act — it puts the blob in the transport — so all-or-nothing would mean either leaving blobs the inscription does not describe (a refused seal) or refusing a publish over history that was never load-bearing.
- **Restore reports dumps and does not consume them.** A restore builds a node standing at `sequence`, so the tip is what it reads; dumps are recorded in the plan, excluded from the byte accounting and from the disk preflight, and reported. Tip completeness is checked on the tip alone. Bootstrapping *at* a retained epoch — consuming a dump as the tip — is deliberately out of scope here and nothing forecloses it: the restore write path takes a kind and a descriptor, and a dump's are the tip's.

Log kinds: one per namespace the ledger writes epoch-boundary logs under, and the set is closed — `log-account-epochs`, `log-epochs`, `log-stakes`. `account-epochs` is one `(account, epoch)` record holding everything an account did in an epoch; it replaced `account-stakes`, `leader-rewards`, `member-rewards` and `pool-deposit-refunds`, which were four identically-keyed namespaces and are now the profile's four retired ones (ADR-0027). The kind token is `log-` followed by the namespace with `_` rewritten to `-`, since a media type's kind token admits hyphens and not underscores; the mapping is injective, and a publisher spells the kinds out rather than composing them, so a namespace rename cannot silently rename a published kind. The namespace is therefore **not** in the record — it is the layer — and a restore writes a layer's records into the namespace its kind names. A publisher that finds logs under a namespace no kind covers **fails the publish** naming the namespace: the format has no layer to carry them, and shipping a snapshot that silently omits a slice of the ledger is the failure that costs most and shows least. Log layers wear the epoch scope unchanged, so per-(kind, scope) inheritance works across the split with no new scope shape.

Empty log layers are omitted: **a `log-{ns}` layer exists if and only if it holds at least one record.** The rule is content-determined and not writer discretion — that is what keeps it deterministic across publishers, since two honest publishers agree about whether a layer exists exactly because they agree about whether a record does. Byron alone sheds ~1,200 empty blobs to it. Absence is normative and never a defect: a restore that finds no `log-stakes` for an epoch restores that epoch with no stake logs, exactly as the ledger had none. Every other kind keeps the arity it had — `blocks` and `indexes` are one layer per epoch whatever the window holds, and every state layer is always present, empty or not.

#### Compatibility contract

Within a media-type version, `.v{x}` is a **contract on record content, not an exact-byte pin** (decision 0026). Writers MAY append numbered optional or `#[cbor(default)]` fields; readers MUST skip fields they do not know; field indexes are never renumbered, removed or repurposed. Every append moves that namespace's `schemas` revision — additive included, because `schemas` is what keeps the inscription a full pin of the bytes rather than a compatibility gate. `v{x}` bumps are reserved for breaking changes.

Enums are the contract's hard edge and the one place the rule inverts. A minicbor enum refuses a variant index it has never heard of, so **adding a variant to any enum reachable from a record is reader-breaking within `v{x}`**, whatever the field policy says: it requires a media-type version bump on the kinds that carry it, or an explicit ADR waiver.

The contract is enforced by `crates/snapshot/tests/field_registry.rs` and its `tests/registry/` data module, which is where a record shape is actually pinned. Per namespace it holds a **canary** — a fully-populated value, every `Option` `Some` and every collection non-empty — and the hex of its encoding through the production encode path, at that namespace's current `schemas` revision. The suite then asserts, on every build:

- the canary still encodes to its pinned bytes, so a renumbering, a removal or a silent width change is a build failure rather than a moved digest with no explanation;
- the registry's current revision equals `SCHEMA_REVS[ns]`, so a field appended without its revision bump — or a bump with nothing pinning it — fails in either direction;
- every **retained** revision still decodes under today's decoder, which is the reader tolerance the contract promises, asserted rather than assumed. Retained canaries are append-only: never edited, never deleted. A change that makes one undecodable is breaking by definition.
- every reachable enum's variant table is pinned per variant, including the Pallas enums the records embed — a dependency upgrade that renumbered `Relay` or `DRep` would otherwise change published bytes with nothing in this repository to notice;
- the codec's own tolerance behaviour (unknown trailing field skipped, index gap null-padded, missing trailing field defaulted) holds against the pinned `minicbor` version, so a codec upgrade that changed any of the three breaks in a test rather than in a published stele;
- the same value encodes to the same bytes across repeated construction. A failure there is never a re-pin: it is an encoding-determinism defect, and it breaks the cross-party digest identity this whole document rests on.

The `digests` layer covers the immutable files fully contained in the stele's block range: `lastImmutable` is derived from the boundary slot and the chunk geometry observed in the chain — canonical, never dependent on aggregator state at publish time. Digest values equal Mithril Cardano DB v2's merkle leaves (hex-decoded), so any Mithril certificate whose beacon covers `lastImmutable` can verify them via the aggregator's digest route and a merkle proof. The certificate reference is deliberately *not* part of the inscription: certificates are produced on the aggregator's cadence, so two independent publishers at the same boundary would reference different certificates — including one would break cross-publisher determinism, while the digest values themselves are byte-stable properties of the chain.

### The cut point and the boundary sliver

A stele is cut by syncing with `chain.stop_epoch = E`, and the halt is **one block past the epoch boundary**, not on it. The sync crosses the boundary — Ewrap closes epoch E-1, Estart opens epoch E — and then applies the first block of epoch E before stopping. That block is what makes the stele addressable: Estart alone leaves the cursor a bare `ChainPoint::Slot` carrying no block hash (`estart::commit_finalize`), and `position.point` must carry one for a stele to be verifiable against a chain. The anchoring block may itself sit exactly on `epoch_start(E)`, in which case the sliver is one slot wide.

So the epoch windows a stele covers are `0..=E`: epochs `0..E-1` **complete**, plus epoch E's **boundary sliver** — the window that opens at `epoch_start(E)` and closes at the anchoring block. The sliver is normative, not an artifact of where a publisher happened to stop.

It is load-bearing because of how boundary data is keyed. **All of epoch X's boundary logs key at `epoch_start(X)`**: Ewrap writes the *ending* epoch's closing logs and its completed `EpochState` at `epoch_start(X)` when X is the epoch it closes, and Estart writes the *starting* epoch's opening account logs at `epoch_start(X)` when X is the epoch it opens. One temporal key per epoch, and nothing straddles two windows. Epoch E's estart logs therefore live inside the sliver and nowhere else: a stele that dropped the sliver in the name of a clean boundary would ship a state tip whose opening logs are in no layer at all, and a restored node would never regenerate them.

`sequence`, the immutable tag's E, and `position.epoch` are consequently **one number**: the epoch the cursor stands in, which is also the last epoch the layers cover. The operator-facing form of the same rule is *configure the epoch you want the node to start in* — `stop_epoch = E` produces the stele tagged `epoch-E`.

### OCI layout and the inscription

- Repository per (profile, network) — e.g. `ghcr.io/txpipe/dolos-snapshots/mainnet`; tags `epoch-E` (E = the newly started epoch, equal to `sequence` and to `position.epoch`; layers cover epochs `0..E-1` complete plus epoch E's boundary sliver — see "The cut point and the boundary sliver" above) and `latest`. The protocol requires an immutable tag per sequence plus a moving `latest`; the profile renders the strings.
- `artifactType: application/vnd.stelae.stele.v1`; layer media types per the table above; three annotations per layer, named in "The manifest" below — one of them normative, the other two informational.
- Config blob (`application/vnd.stelae.inscription.v1+json`), canonical JSON per RFC 8785. Generic keys plus three profile-owned opaque objects — `position`, `parameters` and each layer's `scope`:

```json
{ "schema": 1,
  "profile": {"name": "io.txpipe.dolos.cardano", "version": 1},
  "sequence": 550,
  "position": { "network": {"magic": 764824073, "name": "mainnet"},
                "point": {"slot": 152236812, "hash": "…"},
                "epoch": 550 },
  "parameters": { "indexKeyHash": "xxh3-64",
                   "shards": {"accounts": 16, "assets": 16, "datums": 16, "utxos": 16, "…": 1},
                   "schemas": {"accounts": 1, "utxos": 1, "…": 1},
                   "stateEpochs": [208, 236, 290, 365] },
  "compression": {"algo": "zstd", "level": 9},
  "history": [
    {"sequence": 548, "inscriptionDigest": "sha256:…"},
    {"sequence": 549, "inscriptionDigest": "sha256:…"} ],
  "layers": [
    {"kind": "blocks", "mediaType": "application/vnd.dolos.stele.blocks.v1+zstd",
     "diffId": "sha256:…", "records": 21600, "uncompressedSize": 43210000,
     "scope": {"epoch": 0, "startSlot": 0, "endSlot": 21599}},
    {"kind": "state-utxos", "mediaType": "application/vnd.dolos.stele.state-utxos.v1+zstd",
     "diffId": "sha256:…", "records": 812345, "uncompressedSize": 402653184,
     "scope": {"shard": 0}},
    {"kind": "state-pools", "mediaType": "application/vnd.dolos.stele.state-pools.v1+zstd",
     "diffId": "sha256:…", "records": 3210, "uncompressedSize": 1048576,
     "scope": {"shard": 0}},
    {"kind": "state-pools", "mediaType": "application/vnd.dolos.stele.state-pools.v1+zstd",
     "diffId": "sha256:…", "records": 2980, "uncompressedSize": 972800,
     "scope": {"epoch": 365, "shard": 0}},
    {"kind": "digests", "mediaType": "application/vnd.dolos.stele.digests.v1+zstd",
     "diffId": "sha256:…", "records": 6188, "uncompressedSize": 618800,
     "scope": {"lastImmutable": 6187}} ] }
```

`parameters` is the profile's compatibility declaration. Three of its four values are a consequence of publisher code rather than a free choice: `indexKeyHash` names the hash behind the pre-hashed index keys; `shards` is the per-namespace shard map above; and `schemas` is a per-namespace revision of the *record content* — the stored minicbor a `state-{ns}` or `log-{ns}` layer carries verbatim — which moves when that namespace's stored shape changes, plus one entry at revision `0` per retired namespace, per the removed-kind rule above. Thirteen of the fourteen live revisions are 1; `epochs` is at 2, the first bump the format has taken (see Limitations), and the four retired namespaces sit alongside them at 0. Every live revision is pinned by a canary in `crates/snapshot/tests/field_registry.rs`, which fails the build when a record's field table moves without its revision, or the other way round. The split between the two is deliberate: a change to how a layer is *framed* moves that kind's media type and fails closed at the transport, while a change to what a record *contains* moves its schema revision, which a reader consults to decide whether it can interpret what it can already parse. The fourth, `stateEpochs`, is the exception that proves the rule: it is the publisher's configured retained set, and it is here precisely *because* it is a choice — declaring it is what turns a configuration difference between two publishers into a visible parameters difference instead of a silently divergent history.

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

The arithmetic is counted in layers, because layers are what the ceiling counts: a descriptor with its annotations costs ~350 bytes, so the ceiling falls near 12,000 layers. A mainnet stele is bounded above by ~600 epochs × 5 per-epoch kinds (`blocks`, `indexes` and the three `log-{ns}`), plus the state tip's 74 layers (4 namespaces × 16 shards + 10 single blobs), plus 74 more for every retained state dump — at 20 retained epochs, the ceiling of what a publisher is expected to configure, that is ~4,554 layers and a manifest of roughly 1.6 MB, still comfortably inside the ceiling. The bound is loose in the direction that helps: the log kinds are omitted when empty, and Byron's ~200 epochs carry no reward or stake logs at all, so the realized count sits near ~4,150. **This is the arithmetic that bounds the retained list**, and the reason per-epoch dumps were rejected: ~580 of them would be ~43,000 state layers on their own, more than three times the ceiling. (The Rationale's "~1,700 manifest descriptors" is decision-time sizing of the pre-split artifact; this paragraph is the authoritative count, and it counts layers rather than epochs.)

#### What the transport requires of its host

- **A process that opens a registry client must have installed a process-default rustls `CryptoProvider` first.** The transport ships no crypto backend of its own (`reqwest/rustls-no-provider`): the backend the client library would otherwise pick, `aws-lc-rs`, wants `cmake` on every build machine — the dependency this workspace already goes out of its way to avoid — so it stays out of the tree and the choice of provider moves to the program. In Dolos, `main()` installs `ring`. Omitting the install is a panic when the registry client opens, not a link error.
- **Authentication is the host's decision, in one of three shapes.** The client is opened with credentials its caller supplies — anonymous, a bearer token, or an HTTP Basic pair — and never sources them itself. Which identity a program authenticates as is that program's credential policy, and where it keeps its credentials is that program's deployment: a protocol library that read an environment variable would be deciding both on its host's behalf, and naming the variable would freeze that decision into a published API. **So this specification names no environment variable and no configuration key**, and `stelae::oci::Options::auth` is the whole of the interface. Dolos's own answer is under "CLI and configuration" below.

  Anonymous remains legitimate and is what a genuinely public repository wants. It is not what a registry that authenticates every request wants, and that is the deployment Dolos is heading for: read access to a stele repository is free and identity-less, and still credentialed.

### Code layout

Four crates, all workspace members until the extraction. The Stelae half is two of them — the protocol a third party implements from and the profile-generic lifecycle machinery — and the boundary is checkable: **`cargo tree -e normal --all-features` for `stelae` and `stelae-driver` must contain no `dolos-*` package**, so extracting the pair is a directory move rather than a refactor.

```text
crates/stelae/            # package `stelae` — the wire protocol, zero dolos deps
  lib.rs          # errors, protocol constants, envelope media types
  frame.rs        # deterministic CBOR-seq record read/write, Limits
  codec.rs        # fixed-arity decode helpers for layer content records
  inscription.rs  # schema, JCS encode/verify, digest, history invariant (history_for)
  profile.rs      # Profile trait, layer-kind registry, media-type & tag naming rules
  digest.rs       # streaming sha256 + zstd (diffId + blob digest in one pass)
  layer.rs        # reading a layer without holding it
  plan.rs         # progress file, resume, remaining-bytes accounting
  progress.rs     # Observer: what a transfer says about itself while running
  transport.rs    # the SteleReader/SteleWriter seam and the blob index
  dir.rs          # a stele on a local filesystem
  oci.rs          # feature `oci`: push with blob-skip, pull missing-only, tags, referrers
  tests/toy_profile.rs   # a second, trivial profile — proves the core carries no Dolos assumption

crates/stelae-driver/     # package `stelae-driver` — profile-generic lifecycle, zero dolos deps
  lib.rs          # the driver's Error
  profile.rs      # DriverProfile: the dataset policy stelae::Profile deliberately refuses
  predecessor.rs  # Predecessor/First: what a publish follows and may carry forward
  publish.rs      # the chained-publish lifecycle: open, Tuning, Publishing, Chained, standing
  restore.rs      # Budget/Checkpoint/Outlook: restore bounds and the resume checkpoint
  preflight.rs    # one free-space policy, in both directions
  reporting.rs    # counting layers and records for the two drivers to report
  retry.rs        # bounded patience for an external that fails in bursts
  digests.rs      # the digests-layer codec (Cardano immutable-DB file hashes)

crates/snapshot/          # package `dolos-snapshot` — the io.txpipe.dolos.cardano profile
  lib.rs          # DolosProfile, driver re-exports, profile constants, error mapping
  namespaces.rs   # the closed set of state namespaces a Dolos stele carries
  layers/{blocks,indexes,logs,state,digests}.rs
  export.rs       # stores -> layers, generic over dolos_core::Domain
  restore.rs      # layer selection and restore into store writes
  planning.rs     # one epoch selection, one reading of the plan it produces
  registry.rs     # store-typed publish/preview/restore over OCI; Point, verify, inspect
  publisher.rs    # publishing as a sequence of steps a command drives
  node.rs         # registry auth and scratch-dir policy from node configuration
  backfill.rs     # feature `backfill`: the epoch-at-a-time publisher daemon

crates/mithril/           # package `dolos-mithril` — the aggregator fetch; no stelae dependency
```

The Dolos-side crate keeps the name `snapshot` because that is this project's word for the artifact (`dolos snapshot`, `[snapshot]`, `tests/e2e/snapshot.rs`); `stele` is the protocol's word for the same thing. The planned `sign.rs` (Ed25519 detached signatures, trusted keys, k-of-n) has not been built: signing stays specified above and unimplemented, and nothing else in this section is aspirational.

Everything is built against the engine-agnostic core traits (`ArchiveStore`, `StateStore`, `IndexStore` and their writers, `seed_wal_from_state`, `CardanoConfig.stop_epoch`). The store APIs the initial design listed as missing — `StateStore::iter_utxos()`, epoch-ranged iteration of archive tag/exact records, `IndexWriter::append_prehashed` — have all since landed in `dolos-core` and its backends.

### CLI and configuration

- `dolos snapshot publish [--repo oci://…] [--output-dir DIR] [--epochs N..M] [--dry-run]` — export layers; `--output-dir` writes blobs + inscription to disk, `--repo` pushes with blob-skip and moves tags.
- `dolos snapshot digest` — compute and print the canonical inscription and its sha256 from local stores (what independent verifiers run and sign).
- `dolos snapshot verify | inspect`; `sign --key FILE` belongs to the signature
  phase, which is specified and not yet built ("Code layout" above).
- `dolos bootstrap snapshot` gains source-scheme dispatch: `oci://` → new path; https template / `--file` → legacy tarball, unchanged. `--point epoch-E|latest`; existing `--continue` drives resume; `sync.max_history` bounds how much history is fetched (subsumes the old `full`/`ledger` variants).

```toml
[snapshot]
state_epochs = [208, 236, 290, 365]   # publisher: the retained state dumps;
                                      # signed input, pinned per network
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
2. Sync with `chain.stop_epoch = E` until `StopEpochReached` — the state crosses the boundary and lands on the **first block of epoch E**, the block that gives `position.point` a hash.
3. `dolos snapshot publish` — only the newly closed epoch's layers and epoch E's boundary sliver upload; fresh state layers + inscription; tag `epoch-E`, move `latest`. On networks with a Mithril aggregator, fetch the immutable-file digest list from the aggregator's digest route, verify it against a certificate, and write the `digests` layer for the files within the boundary.
4. Determinism job: an independent runner that synced by any means runs `dolos snapshot digest` and alerts on inscription mismatch.
5. Matching verifiers sign and push referrer signatures; clients enforce k-of-n.

Registry hygiene: keep a trailing window of `epoch-E` tags (e.g. 12); untagged state blobs are reclaimed by registry GC; epoch blobs remain referenced by later manifests. Trust evidence for reclaimed steles survives in the `history` of every later inscription.

### Restore pipeline

1. Resolve tag → manifest → inscription; verify its digest, schema, profile name and major version, network magic and signatures.
2. Plan which layers to consume — no kind is mandatory: ledger-only nodes skip `blocks`/`indexes`/`log-{ns}`, and a layer of a kind this client does not implement is skipped too, reported alongside the epochs `sync.max_history` drops — unless its `scope` marks it `required`, which refuses the restore before a store is opened; a future Mithril-sourced mode fetches block data from an aggregator instead of `blocks` layers, verifying each immutable file against the `digests` layer before the usual decode→append import. Plan the epoch range from `sync.max_history`; diff against the progress file (`<storage.path>/.snapshot-restore.json`, records inscription digest + completed layer diffIds) for `--continue`. Preflight: sum the `uncompressedSize` of the planned layers and fail early if free space at `storage.path` is insufficient; derive download progress and time-remaining estimates from the compressed blob sizes of the layers that remain to be fetched — excluding layers already completed per the progress file or already present locally — so resumed and deduplicated restores report correct totals.
3. Open stores; `IndexStore::initialize_schema()`.
4. Per epoch (checkpointed): fetch + verify `blocks` and each `log-{ns}` the epoch carries → archive appends, commit; fetch `indexes` → pre-hashed appends, commit.
5. State tip: fetch every shard of every `state-{ns}` kind (parallelizable) → dispatch on the kind; `set_cursor(position.point)` last so `has_existing_data()` only ever sees complete restores; commit.
6. Rebuild live-UTxO indexes: `iter_utxos()` → `index_delta_from_utxo_delta` chunks; final chunk aligns the index cursor.
7. Delete progress file; existing `seed_wal_from_state` reseeds the WAL; the daemon chain-syncs the partial current epoch.

Steps 1–2 and the fetch/verify half of steps 4–5 are protocol code; the store writes are profile code.

### Development phases

**1a. Stelae core** — `crates/stelae`: framing, inscription (schema, JCS, digest, history invariant), the `Profile` trait and naming rules, streaming digest/compression, signatures. Verified by CBOR-seq roundtrip and write→read→write byte-identity property tests, a JCS inscription golden test, history-invariant tests (gap/duplicate/out-of-order → reject), fail-closed tests (unknown generic key, unknown profile, higher profile major), and a toy non-Dolos profile exercising the full path.

**1b. Dolos profile core** — `crates/snapshot`: `DolosProfile`, layer readers/writers, and the three trait additions with backend impls and adapter enums. Verified by per-layer roundtrip unit tests and golden-digest tests (fixed input → asserted sha256, catching encoding drift).

**2. Local export/restore e2e** — `export.rs`/`restore.rs` (including the `digests` layer writer), `publish --output-dir`, `bootstrap snapshot --source file://`. Verified by an e2e cloned from `tests/e2e/snapshot.rs`; cross-check restored stores against an `import_blocks`-built node; determinism test (two independently synced nodes → identical inscription digests), which is where any entity-encoding nondeterminism surfaces; audit `crates/cardano/src/model/*` as needed.

**3. OCI transport** — push with blob-skip, pull missing-only, tags. Verified against a local registry (`zot`/`registry:2`) spawned by the test; delta assertions (publish E then E+1 → only new blobs upload; pre-seeded restore fetches only missing); kill-and-`--continue` resume.

**4. Publisher productization** — `digest`/`verify`/`inspect`, `stop_epoch`-driven flow, incremental detection, CI workflow. Verified by a two-runner determinism job on preview/preprod; scheduled preprod publishing before mainnet.

**5. Signatures** — Ed25519 referrers, `trusted_keys`/`require_signatures`. Verified with generated keys plus tampered-layer/inscription negative tests.

**6. Transition** — deprecate the tarball path in docs (keep it working); per-network default `source`. Follow-ups: "refresh" mode for already-running nodes; mid-epoch state-only tip publishes; a Mithril-sourced block restore mode (aggregator download verified against the `digests` layer, reusing the range-download/resume/import machinery from `bootstrap mithril`), which would let public-network snapshots omit `blocks` layers entirely.
