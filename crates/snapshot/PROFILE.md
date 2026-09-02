# The `io.txpipe.dolos.cardano` profile

This document is the normative specification of the Dolos profile of the
[Stelae protocol](https://github.com/txpipe/stelae/blob/main/SPEC.md): the
layer kinds, record shapes, scopes and parameters a `vnd.dolos` stele
carries, and the pipelines that produce and consume one. It began as the
implementation half of `adrs/004_stelae_snapshots.md`, which remains the
decision record — the problem, the adoption of Stelae, and the alternatives —
and now specifies nothing this document covers. Section names are preserved
from the ADR, so older citations land on the same headings here. The crate
beside this file, `dolos-snapshot`, is the implementation.

An independent party reproducing or verifying a Dolos stele reads two
documents: `SPEC.md` for the envelope — framing, inscription, manifest,
transport — and this one for every byte inside it.

## Identity

- Profile name `io.txpipe.dolos.cardano`, version 1; the media-type vendor
  token is `dolos` (IANA `vnd.` custom).
- Payload media types: `application/vnd.dolos.stele.{blocks|indexes|log-{ns}|state-{ns}|digests}.v1+zstd`.
- One repository per network — e.g. `ghcr.io/txpipe/dolos-snapshots/mainnet`.
- Tags: immutable `epoch-E` per sequence plus a moving `latest`, where E is
  the newly started epoch — equal to `sequence` and to `position.epoch`; the
  layers cover epochs `0..E-1` complete plus epoch E's boundary sliver (see
  "The cut point and the boundary sliver").

### Kinds, skips and retirement

The protocol's coexistence rules (SPEC.md, "Profiles, naming and media
types") govern a kind the reader does not implement: rule 3 lets a restore
skip it and report the skip, unless the layer's `scope` marks it
`required`. This profile's own history puts flesh on both edges of that
rule:

Rule 3 answers for a kind the reader does not *know*. It says nothing about a kind the publisher no longer *carries*, and `required: true` cannot be stretched to cover one: `required` is a property of a layer, and a retired kind has no layer to put it on. Absence is already meaningful in this format — a `log-{ns}` layer exists if and only if it holds a record, and a restore passes over a kind it does not recognise — so a reader that still models `log-member-rewards`, finds no such layer, and reports a clean restore has just built a node with no reward history and no way to have noticed.

**A profile therefore declares the namespaces it defines, and a retirement is declared rather than inferred.** `parameters.schemas` carries an entry for every namespace the profile version defines; a namespace it has retired keeps its entry at revision `0`, which is not a schema revision and reads as "this version defines no records here". A restore compares that map against the namespaces it models, before a store is opened: an entry that is missing or zero for one it models refuses the restore and names the namespace. The gate is presence, with revision `0` reading as absence per the sentinel above; a *live* revision's value is never compared — a revision the reader has not seen describes bytes it can still parse, and gating on it would make every additive append breaking, which is exactly what the `.v{x}` contract below exists to avoid.

Like `required`, the rule binds forward and not backward: it constrains readers from the version that implements it onward, and cannot reach the ones already deployed. What protects those, for the four namespaces retired so far, is that every one of them was also a *state* namespace, and the state tip's completeness check refuses a stele missing a kind it expects. A log-only namespace would have had no such backstop, and that is the case this rule exists for. Retiring a namespace is a spec-level act, for the same reason marking one `required` is.

Rule 3's skip is available at layer granularity and at no finer one. Index **dimensions** stay fail-closed: `indexes` is a single layer per epoch, so an unknown dimension surfaces mid-stream — record by record, inside a layer the plan has already committed to restoring — where skipping it would be silent data loss rather than a visible plan-time choice, and where the store cannot look the name up in any case (it keeps a hash of the name, not the name). Changing the dimension set therefore remains a media-type-version event. The same reasoning is why a new *namespace* is additive and a new dimension is not: a namespace arrives as its own `log-{ns}` or `state-{ns}` layer, which a plan can decline; a dimension arrives inside one.

### Layer formats

Framing is the protocol's (SPEC.md, "Layer format"): zstd-compressed CBOR sequences under the pinned deterministic encoding profile, each opening with the protocol-defined header record:

```text
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

State namespaces: the thirteen entity namespaces from `dolos_cardano::model::build_schema()` (key = 32-byte `EntityKey` verbatim, value = stored minicbor verbatim) plus `utxos` (key = `tx_hash(32) ‖ output_index(4, BE)`, value = CBOR `[era: uint, body: bytes]`). The chain point lives in the inscription's `position`, not in a layer. Live-UTxO index dimensions (`utxo::*`) are not shipped; they are rebuilt at restore via `index_delta_from_utxo_delta`.

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

Enums are the contract's hard edge and the one place the rule inverts. A minicbor enum refuses a variant index it has never heard of, so **adding a variant to any enum reachable from a record is reader-breaking within `v{x}`**, whatever the field policy says: it requires a media-type version bump on the kinds that carry it, or an explicit waiver recorded in this document.

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

### Position, parameters and the inscription

The inscription's generic shape, canonicalization and history invariant are
the protocol's (SPEC.md, "The inscription"). What this profile owns are the
three opaque objects — `position`, `parameters` and each layer's `scope` —
and the meaning of `sequence`:

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

`parameters` is the profile's compatibility declaration. Three of its four values are a consequence of publisher code rather than a free choice: `indexKeyHash` names the hash behind the pre-hashed index keys; `shards` is the per-namespace shard map above; and `schemas` is a per-namespace revision of the *record content* — the stored minicbor a `state-{ns}` or `log-{ns}` layer carries verbatim — which moves when that namespace's stored shape changes, plus one entry at revision `0` per retired namespace, per the removed-kind rule above. Thirteen of the fourteen live revisions are 1; `epochs` is at 2, the first bump the format has taken (ADR-004, Limitations), and the four retired namespaces sit alongside them at 0. Every live revision is pinned by a canary in `crates/snapshot/tests/field_registry.rs`, which fails the build when a record's field table moves without its revision, or the other way round. The split between the two is deliberate: a change to how a layer is *framed* moves that kind's media type and fails closed at the transport, while a change to what a record *contains* moves its schema revision, which a reader consults to decide whether it can interpret what it can already parse. The fourth, `stateEpochs`, is the exception that proves the rule: it is the publisher's configured retained set, and it is here precisely *because* it is a choice — declaring it is what turns a configuration difference between two publishers into a visible parameters difference instead of a silently divergent history.

`sequence` is the protocol's ordering key; this profile sets it to the epoch. The three opaque objects are canonicalized by JCS like every generic key (SPEC.md), so determinism holds without the protocol interpreting them — which is why every value in them must itself be deterministic, the property the compatibility contract above enforces.

### The manifest size arithmetic

The 4 MiB ceiling and its measurement are the protocol's (SPEC.md, "The
manifest size ceiling"); what is profile-owned is the arithmetic that keeps a Dolos stele inside the
ceiling, and it stays here: a descriptor with its annotations costs ~350
bytes, so the ceiling falls near 12,000 layers. A mainnet stele is bounded
above by ~600 epochs × 5 per-epoch kinds (`blocks`, `indexes` and the three
`log-{ns}`), plus the state tip's 74 layers (4 namespaces × 16 shards + 10
single blobs), plus 74 more for every retained state dump — at 20 retained
epochs, the ceiling of what a publisher is expected to configure, that is
~4,554 layers and a manifest of roughly 1.6 MB, comfortably inside. The
bound is loose in the direction that helps: the log kinds are omitted when
empty, and Byron's ~200 epochs carry no reward or stake logs at all, so the
realized count sits near ~4,150. **This is the arithmetic that bounds the
retained list**, and the reason per-epoch dumps were rejected: ~580 of them
would be ~43,000 state layers on their own, more than three times the
ceiling. (The Rationale's "~1,700 manifest descriptors" is decision-time
sizing of the pre-split artifact; this paragraph is the authoritative count,
and it counts layers rather than epochs.)

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
