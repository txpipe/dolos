# ADR 004 - Stelae: Deterministic OCI Snapshots

## Status

Proposed

> **This is the decision record, not the specification.** It states the
> problem — the friction of the tarball-shaped snapshot — and the adoption
> of Stelae: the decision, its rationale, its limitations, and the
> alternatives it displaced. The normative text lives elsewhere, split along
> the protocol/profile boundary the decision itself drew: the **protocol**
> in [`SPEC.md` of
> `txpipe/stelae`](https://github.com/txpipe/stelae/blob/main/SPEC.md), and
> the **Dolos profile** in
> [`crates/snapshot/PROFILE.md`](../crates/snapshot/PROFILE.md). Older
> references to this ADR's implementation sections resolve there — PROFILE.md
> preserves the section names. The protocol version this tree implements is
> the tag `crates/snapshot/Cargo.toml` pins, whatever `main`'s spec says
> since.

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

## Adoption

The specification this ADR proposed lives in two normative documents, and
this record deliberately duplicates neither:

- **The protocol** — framing, the inscription and its history invariant, the
  manifest and its agreement rules, the transport and what it requires of a
  host, restore planning — is [`SPEC.md` in
  `txpipe/stelae`](https://github.com/txpipe/stelae/blob/main/SPEC.md), the
  repository the `stelae` and `stelae-driver` crates extracted to.
- **The Dolos profile** — layer kinds and record shapes, scopes, state
  history, `position`/`parameters`, the compatibility contract, the cut
  point, and the publish/restore pipelines — is
  [`crates/snapshot/PROFILE.md`](../crates/snapshot/PROFILE.md), beside the
  `dolos-snapshot` crate that implements it.

What remains below is what only this repository can say: how Dolos adopted
the protocol — where the code lives, what the operator surface is, and the
phases the work shipped in.

### Code layout

The extraction has happened: the protocol crate (`stelae`) and the
profile-generic lifecycle machinery (`stelae-driver`) live in
[`github.com/txpipe/stelae`](https://github.com/txpipe/stelae), history
preserved, and this workspace consumes them as one pinned git tag (both
crates version in lockstep; the pin lives in `crates/snapshot/Cargo.toml`
and nowhere else). Their module layout is documented in that repository;
"no `dolos-*` dependency" is enforced there by its cargo-deny bans — the
boundary this section once asked contributors to keep is structural now.

What remains here is the profile side:

```text
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

- `dolos snapshot publish [--repo oci://…] [--output-dir DIR] [--epochs N..M] [--dry-run]` — export layers; `--output-dir` writes blobs + inscription + the `blobs.json` sidecar to disk, `--repo` pushes with blob-skip and moves tags.
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

### Development phases

**1a. Stelae core** — `crates/stelae`: framing, inscription (schema, JCS, digest, history invariant), the `Profile` trait and naming rules, streaming digest/compression, signatures. Verified by CBOR-seq roundtrip and write→read→write byte-identity property tests, a JCS inscription golden test, history-invariant tests (gap/duplicate/out-of-order → reject), fail-closed tests (unknown generic key, unknown profile, higher profile major), and a toy non-Dolos profile exercising the full path.

**1b. Dolos profile core** — `crates/snapshot`: `DolosProfile`, layer readers/writers, and the three trait additions with backend impls and adapter enums. Verified by per-layer roundtrip unit tests and golden-digest tests (fixed input → asserted sha256, catching encoding drift).

**2. Local export/restore e2e** — `export.rs`/`restore.rs` (including the `digests` layer writer), `publish --output-dir`, `bootstrap snapshot --source file://`. Verified by an e2e cloned from `tests/e2e/snapshot.rs`; cross-check restored stores against an `import_blocks`-built node; determinism test (two independently synced nodes → identical inscription digests), which is where any entity-encoding nondeterminism surfaces; audit `crates/cardano/src/model/*` as needed.

**3. OCI transport** — push with blob-skip, pull missing-only, tags. Verified against a local registry (`zot`/`registry:2`) spawned by the test; delta assertions (publish E then E+1 → only new blobs upload; pre-seeded restore fetches only missing); kill-and-`--continue` resume.

**4. Publisher productization** — `digest`/`verify`/`inspect`, `stop_epoch`-driven flow, incremental detection, CI workflow. Verified by a two-runner determinism job on preview/preprod; scheduled preprod publishing before mainnet.

**5. Signatures** — Ed25519 referrers, `trusted_keys`/`require_signatures`. Verified with generated keys plus tampered-layer/inscription negative tests.

**6. Transition** — deprecate the tarball path in docs (keep it working); per-network default `source`. Follow-ups: "refresh" mode for already-running nodes; mid-epoch state-only tip publishes; a Mithril-sourced block restore mode (aggregator download verified against the `digests` layer, reusing the range-download/resume/import machinery from `bootstrap mithril`), which would let public-network snapshots omit `blocks` layers entirely.
