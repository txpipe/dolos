# The `io.txpipe.dolos.cardano` profile

**Status: frozen for Dolos v2** — profile version 1, payload media-type
version 1 (`.v1`), every namespace at schema revision 1. Every change from
here on follows decision 0026: an encoding change moves its namespace's
revision, the field registry is append-only, and a retirement uses
revision 0 (decision 0063).

This document is the normative specification of the Dolos profile of the
[Stelae protocol](https://github.com/txpipe/stelae/blob/main/SPEC.md): the
layer kinds, record shapes, scopes and parameters a `vnd.dolos` stele
carries, and the pipelines that produce and consume one.
`adrs/004_stelae_snapshots.md` is the decision record — the problem, the
adoption of Stelae, and the alternatives — and specifies nothing this
document covers. The crate beside this file, `dolos-snapshot`, is the
implementation.

An independent party reproducing or verifying a Dolos stele reads two
documents: `SPEC.md` for the envelope — framing, inscription, manifest,
transport — and this one for every byte inside it.

## Identity

- Profile name `io.txpipe.dolos.cardano`, version 1; the media-type vendor
  token is `dolos` (IANA `vnd.` custom).
- Payload media types: `application/vnd.dolos.stelae.{blocks|indexes|log-{ns}|entity-{kind}|state-{ns}|digests}.v1+zstd`.
- One repository per network — e.g. `ghcr.io/txpipe/dolos-snapshots/mainnet`.
- Tags: immutable `epoch-E` per sequence plus a moving `latest`, where E is
  the newly started epoch — equal to `sequence` and to `position.epoch`; the
  layers cover epochs `0..E-1` complete plus epoch E's boundary sliver (see
  "The cut point and the boundary sliver").

### Kinds, skips and retirement

The protocol's coexistence rules (SPEC.md, "Profiles, naming and media
types") govern a kind the reader does not implement: rule 3 lets a restore
skip it and report the skip, unless the layer's `scope` marks it
`required`. Two edges of that rule need spelling out:

Rule 3 answers for a kind the reader does not *know*. It says nothing about a kind the publisher no longer *carries*, and `required: true` cannot be stretched to cover one: `required` is a property of a layer, and a retired kind has no layer to put it on. Absence is already meaningful in this format — a `log-{ns}` layer exists if and only if it holds a record, and a restore passes over a kind it does not recognise — so a reader that still models a retired namespace, finds none of its layers, and reports a clean restore has just built a node missing that namespace's history and no way to have noticed.

**A profile therefore declares the namespaces it defines, and a retirement is declared rather than inferred.** (The entity kinds follow the same rule through `parameters.entities`, below.) `parameters.schemas` carries an entry for every namespace the profile version defines; a namespace it has retired keeps its entry at revision `0`, which is not a schema revision and reads as "this version defines no records here". A restore compares that map against the namespaces it models, before a store is opened: an entry that is missing or zero for one it models refuses the restore and names the namespace. The gate is presence, with revision `0` reading as absence per the sentinel above; a *live* revision's value is never compared — a revision the reader has not seen describes bytes it can still parse, and gating on it would make every additive append breaking, which is exactly what the `.v{x}` contract below exists to avoid.

Like `required`, the rule binds forward and not backward: it constrains readers from the version that implements it onward, and cannot reach the ones already deployed. Retiring a namespace is a spec-level act, for the same reason marking one `required` is.

Rule 3's skip is available at layer granularity and at no finer one. Index **dimensions** stay fail-closed: `indexes` is a single layer per epoch, so an unknown dimension surfaces mid-stream — record by record, inside a layer the plan has already committed to restoring — where skipping it would be silent data loss rather than a visible plan-time choice, and where the store cannot look the name up in any case (it keeps a hash of the name, not the name). Changing the dimension set is therefore a media-type-version event. The same reasoning is why a new *namespace* is additive and a new dimension is not: a namespace arrives as its own `log-{ns}` or `state-{ns}` layer, which a plan can decline; a dimension arrives inside one.

### Layer formats

Framing is the protocol's (SPEC.md, "Layer format"): zstd-compressed CBOR sequences under the pinned deterministic encoding profile, each opening with the protocol-defined header record:

```text
[format_version = 1, profile: tstr, kind: tstr, scope: any]
```

`scope` is opaque to the protocol. The Dolos profile encodes `[network_magic, epoch, start_slot, end_slot]` for epoch layers, `[network_magic, epoch, shard]` for every state layer — one shape across every state kind, single-blob namespaces included, whose one layer is shard 0 — and `[network_magic, epoch, last_immutable]` for the digests layer.

The state layers carry **two roles over that one header shape**, and only the *descriptor* scope tells them apart: a tip is `{"shard": n}`, a retained dump is `{"epoch": E, "shard": n}`. The header is deliberately blind to the distinction, and that is what makes the dump a publish cuts at `sequence == E` the tip's own bytes rather than a copy of them — same header, same records, one `diffId`, one blob under two descriptors. See "State history" below.

Content records per kind (Dolos profile):

| Kind | Record | Order | Restore write path |
|---|---|---|---|
| `blocks` (per epoch) | `[slot, hash: bytes(32), body: bytes]`, body = raw wire CBOR verbatim | ascending slot, stream order for same-slot (Byron EBB) | `ArchiveWriter::apply` |
| `indexes` (per epoch) | tags: `[0, dimension: tstr, key_hash: bytes(8), slot]` with `key_hash = xxh3_64(key)` BE for every dimension; exact: `[1, kind: tstr, key: bytes, slot]` for block-hash/block-number/tx | sorted, deduped | new `ArchiveWriter::append_prehashed` |
| `log-{ns}` (per epoch, per log namespace, omitted when empty) | `[log_key: bytes, value: bytes]`, log_key = `slot(8, BE) ‖` the namespace's typed key (see key layouts), value = stored EntityValue verbatim | `log_key` | `ArchiveWriter::write_log` into the namespace the kind names |
| `entity-{kind}` (per epoch, per entity kind, omitted when empty) | `[id: bytes, slot]`, id = the entity's id verbatim at its kind's width, slot = where it was first seen, inside the window | `id`, strictly ascending | `ArchiveWriter::write_entity` under the kind the layer kind names |
| `state-{ns}` (tip or retained dump, per state namespace, `scope.shard` = 0..`parameters.shards[ns]`-1) | `[key: bytes, value: bytes]` | `key`; shard = first nibble of the namespace's shard byte for a 16-way namespace (`key[0]` for `utxos` and `datums`, `key[4]`, the first credential-hash byte, for `accounts`), 0 for a single blob | dispatch on the kind: `state-utxos` → chunked `StateWriter::apply_utxoset` and derived `apply_utxo_tags` in the same commit, else `write_entity` into the namespace the kind names |
| `digests` (tip, optional) | `[immutable_number, chunk: bytes(32), primary: bytes(32), secondary: bytes(32)]`, each sha256 over the raw file bytes | ascending `immutable_number` | none — verification metadata, not written to stores |

Every tag dimension carries `xxh3_64` of its logical key in `key_hash`, so `parameters.indexKeyHash` describes every dimension. The stores keep only that hash, and the layer ships the stored form — that is the whole point of the pre-hashed design.

State namespaces: the entity namespaces from `dolos_cardano::model::build_schema()` (key = the stored bytes of the namespace's typed key, value = the stored value verbatim, under the [value encoding](#value-encoding)) plus `utxos` (key = `tx_hash(32) ‖ output_index(4, BE)`, value = CBOR `[era: uint, body: bytes, slot: uint, tx_index: uint]`, the last two the UTxO's creation position: the slot of the block holding the producing transaction and that transaction's index in it; a UTxO no block produced — genesis, AVVM, configured — has no position and is `[era: uint, body: bytes]`). The chain point lives in the inscription's `position`, not in a layer. Live-UTxO tags (`utxo::*`) are not shipped; they are rebuilt at restore into the state store via `utxo_index_delta_from_utxo_delta`, keyed `[dim_hash:8][lookup_key][slot:8 BE][tx_index + 1:2 BE][txo_ref:36]` from the position each record carries (ten zero bytes without one), so a restored node lists UTxOs in creation order with no archive read.

A `utxos` value's `era` is a number from the table below. For an output a block produced it is Pallas's encoding of the output's era (`MultiEraOutput::era()`); for a UTxO no block produced it is set as stated after the table. A restore refuses a value whose era is outside the table.

| `era` | Era |
|---|---|
| 1 | Byron |
| 2 | Shelley |
| 3 | Allegra |
| 4 | Mary |
| 5 | Alonzo |
| 6 | Babbage |
| 7 | Conway |

A UTxO no block produced carries the era of how it was built: 1 for a Byron genesis or AVVM output, 7 for a Shelley-genesis output, and for a configured UTxO the era its configuration names, 7 when it names none. A configured era must be one of 1 to 7: the publisher writes it as configured without checking it, and a restore refuses any other, so a node configured with an era outside the table publishes steles that do not restore.

Key kinds: every key below is built from these, and a credential has a different encoding in each kind that carries one. The encodings are fixed as tabled; none is converted into another.

| Kind | Bytes | Width |
|---|---|---|
| `AccountKey` | the stake credential's CBOR: `0x82 ‖ variant ‖ 0x58 0x1c ‖ hash(28)`, variant `0x00` for a key hash and `0x01` for a script hash | 32 B |
| `DRepKey` | the CIP-129 header, `0x22` for a key hash and `0x23` for a script hash, `‖ hash(28)`; abstain is the single byte `0x00` and no-confidence `0x01` | 29 B or 1 B |
| `PoolKey` | the operator's key hash, raw | 28 B |
| `PolicyKey`, `PaymentKey`, `ScriptKey` | the policy, payment credential or script hash, raw, with no key-or-script header | 28 B |
| `ProposalKey` | `tx(32) ‖ idx(4, BE)`, the CIP-129 governance action id order | 36 B |
| `AssetKey` | `blake2b256(policy ‖ name)` | 32 B |
| `DatumKey` | the datum hash | 32 B |
| `AddressKey` | `AccountKey ‖ rest` for an address with a stake credential, else `0x00 ‖ address`; the forms are told apart by the first byte, which relies on every `AccountKey` opening with `0x82` | 61 B for a base address |
| `EraKey` | protocol `u16`, BE | 2 B |
| `GroupKey<G, M>` | `group ‖ epoch(8, BE) ‖ member`; `Ungrouped` is zero bytes | the sum |
| `stake` entity id | the stake address the `stake` archive tag files a credential under: its header byte (`0xe0`/`0xf0` for a key/script credential on a testnet, `0xe1`/`0xf1` on mainnet) `‖ hash(28)` | 29 B |

Inside values, credentials appear in two more places. The `drep_votes` and `cc_votes` maps of `proposals` (and every credential-keyed map of `gov`) key by the stake credential's CBOR, the same bytes as an `AccountKey`; the `spo_votes` map keys by the 28-byte pool hash. `gov`'s DRep distribution keys by Pallas's `DRep` CBOR: `[0, hash]` key, `[1, hash]` script, `[2]` abstain, `[3]` no-confidence.

Key layouts (decision 0051): every key is the encoding of one typed key struct per namespace, of whatever length its kind has, and both codecs refuse a record whose key does not decode exactly as its namespace's kind — so a key padded, truncated or borrowed from another namespace fails at the record rather than restoring into a row nothing reads. A log key is the slot followed by the log's key kind. Integers are big-endian.

| Namespace | State key | Log key, after `slot(8)` |
|---|---|---|
| `accounts`, `pending-rewards`, `pending-mirs`, `pruned-accounts` | `AccountKey`: CBOR stake credential, `0x82 ‖ variant ‖ 0x58 0x1c ‖ hash(28)`, 32 B | — |
| `account-address-chunks` | — | `AccountKey`, 32 B |
| `account-addresses` | `GroupKey<AccountKey, AddressKey>`: `account(32) ‖ epoch(8) ‖` the `address-stats` log key, last, 101 B for a base address | — |
| `account-epochs` | — | `AccountKey`, 32 B |
| `account-stats` | `(Epoch, AccountKey)`: `epoch(8) ‖ AccountKey`, 40 B | `AccountKey`, 32 B |
| `address-stats` | `(Epoch, AddressKey)`: `epoch(8) ‖` the log key, 69 B for a base address | `AddressKey`: `AccountKey ‖ rest` for an address with a stake credential, `rest` being the address without its stake hash (a base address's header and payment hash, a stake address's header), else `0x00 ‖ address`; the first byte, `0x82` or `0x00`, tells the forms apart; 61 B for a base address |
| `asset-mint-chunks` | — | `AssetKey ‖ u32`: `asset key(32) ‖ chunk(4)`, 36 B |
| `asset-mints` | `GroupKey<AssetKey, (u64, u32)>`: `asset key(32) ‖ epoch(8) ‖ slot(8) ‖ tx index(4)`, 52 B | — |
| `datums` | `DatumKey`: datum hash, 32 B | — |
| `drep-epochs` | — | `DRepKey`, as `dreps`: 29 B for a credential, 1 B for abstain and no-confidence |
| `drep-sightings` | `DRepKey`, as `dreps` | — |
| `dreps` | `DRepKey`: `0x22` (key) or `0x23` (script) `‖ hash(28)`, 29 B; `0x00` abstain, `0x01` no-confidence, 1 B | `DRepKey`, as state |
| `epochs` | `()`: empty | `()`: empty |
| `eras` | `EraKey`: protocol `u16`, 2 B | — |
| `first-mint-chunks` | — | chunk `u32`, 4 B |
| `first-mints` | `GroupKey<PolicyKey, AssetKey>`: `policy(28) ‖ epoch(8) ‖ asset key(32)`, 68 B | — |
| `first-script-chunks` | — | chunk `u32`, 4 B |
| `first-scripts` | `GroupKey<Ungrouped, ScriptKey>`: `epoch(8) ‖ script hash(28)`, 36 B | — |
| `gov` | `()`: empty | — |
| `metadata-labels` | label `u64`, 8 B | — |
| `payment-address-chunks` | — | `PaymentKey`: payment hash, 28 B |
| `payment-addresses` | `GroupKey<PaymentKey, PaymentAddressKey>`: `payment(28) ‖ epoch(8) ‖` the address without its payment hash, last, 65 B for a base address | — |
| `payment-stats` | `GroupKey<PaymentKey, ()>`: `payment(28) ‖ epoch(8)`, 36 B | — |
| `policy-first-mint-chunks` | — | `PolicyKey ‖ u32`: `policy(28) ‖ chunk(4)`, 32 B |
| `pool-epochs` | — | `PoolKey`, 28 B |
| `pools` | `PoolKey`: operator hash, 28 B | `PoolKey`, 28 B |
| `proposals` | `ProposalKey`: `tx(32) ‖ idx(4)`, 36 B | `ProposalKey`, 36 B |
| `utxos` | `tx_hash(32) ‖ output_index(4)`, 36 B | — |

A state row under a log-only namespace (`—` in the state key column) is never written; one that arrives is held to the log's key kind.

Namespace names follow one grammar. A name says what a row is, and the suffix says how it is kept:

| Form | Meaning | Namespaces |
|---|---|---|
| a plural noun | one state row per thing | `accounts`, `datums`, `drep-sightings`, `dreps`, `eras`, `metadata-labels`, `pending-mirs`, `pending-rewards`, `pools`, `proposals`, `pruned-accounts`, `utxos` |
| `epochs`, `gov` | as named: one `EpochState` row (the tip in state, each closed epoch in the log), and the one `GovState` | `epochs`, `gov` |
| `{thing}-epochs` | a log of one row per thing and epoch | `account-epochs`, `drep-epochs`, `pool-epochs` |
| `{thing}-stats` | a per-epoch sum row that may grow fields at new indices; sealed into a log of the same name, or folded into a chunk | `account-stats`, `address-stats` (sealed under their own names); `payment-stats` (folded into `payment-address-chunks` as its `tx_count`) |
| a `Set`'s plural | a state-only `Set` (decision 0057), one row per member keyed `group ‖ epoch ‖ member` | `account-addresses`, `asset-mints`, `first-mints`, `first-scripts`, `payment-addresses` |
| `-chunks` | the sealed list of a `Set`, one per epoch, or one per group and epoch; `{group}-{member}-chunks` names its group and member | `first-mint-chunks`, `first-script-chunks` (ungrouped); `account-address-chunks`, `asset-mint-chunks`, `payment-address-chunks`, `policy-first-mint-chunks` |
| a state name, as a log | final rows: the last state row of a thing that left the state namespace | `log-dreps`, `log-pools`, `log-proposals` |

`state-gov` carries the one `GovState` value. Fields 6 and 7, `distr` and `prev_distr`, are the stake distributions EWRAP accumulates over its account shards, as `[closing_epoch, committed_shards, total_shards, drep_distr, pool_distr, pool_total]`. The two shard counters are the accumulator's bookkeeping, and their only published meaning is that they are equal: a stele carries a distribution only with every shard merged, and the boundary check refuses a tip where they differ. Field 9, `dissolved_committee`, is the last seated committee while a no-confidence keeps the committee dissolved, as `[committee, auths, action, epoch, seated_by]`: the members and threshold, each member's authorization history as it stood, the enacted `NoConfidence` action id, the first epoch without a seated committee, and the action id that seated the committee, absent for the Conway-genesis committee, which leaves a four-element array. It is absent while a committee is seated.

`state-pools` carries `PoolState` values. Field 4, `calidus`, is the pool's highest-nonce CIP-151 registration signed by its cold key in a valid transaction, as `[pub_key, nonce, tx_hash, slot]`: the 32-byte public key, or null for a revocation (an all-zero key), the registration's nonce, and the transaction and slot that made it. A registration replaces it only with a higher nonce, so a revocation keeps its nonce. It is absent before any registration.

`epochs` carries `EpochState` values: the tip in `state-epochs`, and in `log-epochs` each closed epoch as EWRAP left it, keyed at the epoch's start. Field 12, `active_stake`, is the epoch's active stake: the distribution frozen when epoch `number - 2` closed, summed when the epoch opened. Fields 9, 10 and 11 (`ewrap_progress`, `estart_progress`, `rupd_progress`), 13 (`estart_tally`) and 14 (`ewrap_finalized`) are the sync cursors, crash-recovery state a boundary pass keeps while it runs. They are null, and field 14 false, in every published row: EWRAP writes the `log-epochs` row with them cleared, and the boundary check refuses a tip that has any set.

`proposals` carries `ProposalState` values, live in `state-proposals` and final in `log-proposals`. Field 3, the action, is one of seven variants, `ParamChange`, `HardFork`, `TreasuryWithdrawal`, `NoConfidence`, `UpdateCommittee`, `NewConstitution` and `Info`, at indexes 0 to 6. Field 9, `proposed_in`, the epoch the proposal was submitted in, is always present. Fields 10 to 12 (`parent`, `purpose`, `anchor`) are null when the action has none, and fields 13 to 15 hold the committee, DRep and pool votes.

`log-pool-epochs` carries one `PoolEpochLog` per pool and epoch `e`, keyed at `epoch_start(e)`, as `[blocks_minted, total_stake, delegators_count, live_pledge, total_rewards, fixed_cost, margin_cost]`. `total_stake` is the pool's active stake in `e`. Its share of the `active_stake` in `e`'s `log-epochs` row is the pool's relative size, which a reader derives: the row stores no ratio.

`state-datums` carries `DatumState` values as CBOR `[refcount: uint, bytes: bytes]`, keyed by the datum hash, with `bytes` the datum's CBOR as the witness set carried it. The count follows one rule, applied per transaction, the transaction's consumed outputs before its produced outputs: first each consumed output whose datum option is a hash takes one away, deleting the row at 0, and does nothing when there is no row; then each produced output whose datum option is a hash `h`, in a transaction whose witness set carries the datum for `h`, adds one, and creates the row at 1 with that witness's bytes when there is none. Inline datums are not counted, and neither is an output naming a hash whose datum its transaction does not carry. The count is what this rule yields over the chain, which is not always the number of live outputs naming the datum.

`state-metadata-labels` carries `MetadataLabelState` values as CBOR `[tx_count: uint]`, with `tx_count` at field 0. Its key is the metadata label as an exact 8-byte big-endian u64.

`state-pruned-accounts` carries `PrunedAccountState` values as CBOR `[lifetime_rewards: uint, deregistered_at: uint / null]`, keyed by `AccountKey`. ESTART deletes an account that is dead once rotated (unregistered, no balance, every stake snapshot empty and every delegation snapshot undelegated, no retired pool) from `state-accounts`, unless it deregistered in the closing epoch, in which case a later boundary deletes it; it adds the deleted account's history here: its lifetime rewards, which equal its lifetime withdrawals since a dead row holds no reward balance, summed over every pruned life of the credential, and its newest deregistration slot. A credential's row exists only once it was pruned while carrying rewards or a deregistration; the API reads it, and no sync pass does.

State kinds: one per state namespace, and the set is closed — each spelled `state-` followed by the namespace with `_` rewritten to `-`, by the same rule and for the same reasons as the log kinds below. The namespace is therefore **not** in the record — it is the layer — which is what puts the fail-closed edge of a breaking change on exactly the namespace that broke, and lets a reader skip a namespace this profile does not define at the transport rather than choking on one shared layer. The shard count is **specification, never tuning**: `utxos`, `accounts` and `datums` split 16 ways, every other namespace is a single blob, and `parameters.shards` reports the map so a reader never has to discover it from the data. A 16-way namespace shards on the high nibble of its **shard byte**, the first key byte that comes from a hash: byte 0 for `utxos` (the transaction hash) and `datums` (the datum hash), byte 4 for `accounts`, whose keys open with the stake credential's CBOR header `0x82 ‖ variant ‖ 0x58 0x1c` and so carry no entropy until the hash. The shard bytes are fixed by the profile, like the counts, and are not declared in `parameters`. Re-sharding a namespace is a media-type-version event for that namespace's kind. Every shard of every kind is published, empty ones included, so tip completeness is structural: a restore requires every state kind and, per kind, exactly the shards its count promises.

**State history: retained dumps at configured epochs, plus the moving tip.** A stele's state is the tip — the ledger as of `sequence`, swapped whole by every publish — and, for each epoch a publisher retains, an immutable **dump** of the state as of that epoch. The two are the same kinds, the same records and the same shard geometry; a dump differs from a tip in its descriptor scope, which names the epoch, and in nothing else.

- **The retained set is a publisher input, not derivation.** The publisher names it (`--state-epochs` on `dolos snapshot digest` and `verify --reproduce`). Era boundaries are one sensible criterion and cherry-picked epochs another; which epochs are worth a dump is operational, so nothing derives the list from the chain summary. The list is strictly ascending and never names epoch 0, and it is refused where it is read rather than where a dump is cut — it reaches `parameters` before any layer is written. Publishers are expected to keep it around 10–20: a dump per epoch would put ~50k descriptors in a mainnet manifest against a ~12k ceiling.
- **The list is signed input.** It is echoed verbatim into `parameters.stateEpochs`, so two publishers of one network configured differently produce different parameters, different inscription digests, and a divergence an operator reads out of a one-field diff instead of hunting through layers for. It is therefore **normative per network** and belongs pinned wherever the default repository is pinned: a publisher quietly running a different list self-ejects from co-signing.
- **Production rule.** At the publish where `sequence` equals a retained epoch E, E's dump is cut from the tip: one walk of the store, one sink per shard, one blob, and the transport attests the result a second time under the dump's scope. Nothing is compressed twice and nothing crosses the wire twice. At a publish standing past E, the dump is adopted from the predecessor by the same scope equality every immutable layer uses. A dump for a past epoch that no predecessor carries is a **warning and a shorter stele**, never a failed publish: this publish's stores hold the tip, and the state as of an epoch it has moved past is not in them to be written. Producing one is a backfill run's job.
- **Inheritance.** A tip layer is never inherited, for two independent reasons: the tip changes every publish, and its scope names no epoch, so scope equality could not tell one publish's shard from another's. A dump's scope does name its epoch, so it inherits, checkpoints and resumes exactly as a `blocks` layer does — including through the resumption record an interrupted publish leaves.
- **Adoption is per shard.** A predecessor that can hand over fifteen of a dump's sixteen shards hands over fifteen. Adopting is an act — it puts the blob in the transport — so all-or-nothing would mean either leaving blobs the inscription does not describe (a refused seal) or refusing a publish over history that was never load-bearing.
- **Restore reports dumps and does not consume them.** A restore builds a node standing at `sequence`, so the tip is what it reads; dumps are recorded in the plan, excluded from the byte accounting and from the disk preflight, and reported. Tip completeness is checked on the tip alone. Bootstrapping *at* a retained epoch — consuming a dump as the tip — is deliberately out of scope here and nothing forecloses it: the restore write path takes a kind and a descriptor, and a dump's are the tip's.

Entity kinds: one per entity kind the chain declares for the archive's entity index (decision 0064) — `entity-asset`, `entity-script` and `entity-stake`. The entity index maps an entity's id to the slot it was first seen at; it is complete for all history, written by the seal of an immutable epoch in the same commit as the epoch's sealed logs, and never undone. It is what tells a script or asset's first appearance from a repeat, and whether a stake credential was ever seen. `asset` ids are the 32-byte `AssetKey`, `script` ids the 28-byte script hash, `stake` ids the 29-byte stake address the `stake` archive tag files a credential's addresses under, stored verbatim. The kind token is `entity-` followed by the kind's name, and the kind is **not** in the record — it is the layer — so a reader that does not register a kind skips its layers at the transport (rule 3) and reports them. A window's layer holds the entities first seen in it, in id order, and exists if and only if it holds a record. Epoch `e`'s entries are written by the seal (see "The seal" below) at the boundary that seals `e`, so the stele cut there is the first whose `e` window carries them, exactly as for `log-first-script-chunks`; until then the epoch's first sightings ride in the state tip as `state-first-scripts`, `state-first-mints` and `state-account-addresses`. A stele at sequence `E` therefore holds entries for the windows the boundary opening `E` or an earlier one sealed. An epoch seals in one ESTART, so a window's layer is whole or absent and inherits per `(kind, scope)` like every other closed window. An entry's slot is its value, so no traversal can seek to an epoch: export scans the index once to learn which windows hold an entry, then once per band of windows it builds, routing each entry to its window, banded like the `indexes` layers.

`parameters.entities` declares every entity kind the publisher writes, with its id width. Absence of a layer cannot say that a publisher does not carry a kind, so the declaration does: a restore refuses, before a store is opened, a stele that does not declare a kind the reader registers or declares it at another width, since a node restored without that kind's entries would take every repeat for a first sighting. A declared kind the reader does not register is skipped and reported like any unknown kind.

Log kinds: one per namespace the ledger writes epoch-boundary logs under, and the set is closed — `log-account-address-chunks`, `log-account-epochs`, `log-account-stats`, `log-address-stats`, `log-asset-mint-chunks`, `log-drep-epochs`, `log-dreps`, `log-epochs`, `log-first-mint-chunks`, `log-first-script-chunks`, `log-payment-address-chunks`, `log-policy-first-mint-chunks`, `log-pool-epochs`, `log-pools`, `log-proposals`. `account-epochs` is one `(account, epoch)` record holding everything an account did in an epoch. `drep-epochs` holds one row per entry of each completed DRep distribution, Abstain and NoConfidence included, as `[voting_power, registered]`; the distribution the boundary closing epoch `n` completes keys at `epoch_start(n)`, like `epochs`, and Koios labels it `epoch_no = n + 1`, the key's epoch plus one. `proposals` is a final-row log: a resolved proposal leaves the `proposals` state namespace at the boundary after the one that resolved it, and its last state row, without the three vote maps, is logged under the same namespace, keyed at `epoch_start(e)` for the epoch `e` that boundary closes — the proposal's enacted or dropped epoch. The row is written by the boundary that opens `e + 1`, so the first stele whose `e` window is closed carries it, and the window never gains a row after that stele. `pools` is a final-row log on the same rule: a pool whose every snapshot is retired once rotated leaves the `pools` state namespace at the boundary opening its retiring epoch `r + 3`, and its last state row is logged under the same namespace, keyed at `epoch_start(r + 2)`. A pool that registers again gets a fresh state row, and one that retires again logs another row under its new epoch. `dreps` is a final-row log the same way: a DRep unregistered, with its deposit refunded, before the epoch a boundary closes and before every proposal still live was submitted leaves the `dreps` state namespace at that boundary, and its whole last state row is logged under the same namespace at `epoch_start(e)` for the closed epoch `e`. A later registration makes a fresh state row.

`account-epochs` is the one log whose closed windows change. ESTART opening epoch `e` writes `e`'s rows at `epoch_start(e)` with the stake leg alone, and the EWRAP closing `e + 1` rewrites them in full under the same keys at the boundary opening `e + 2`. Window `e`'s `log-account-epochs` layer therefore **settles** in the stele cut at `e + 2`: the steles at `e` (the boundary sliver) and `e + 1` carry the stake-only rows, and every later stele the full rows, the same rows a node synced to that boundary holds. A publish carries a predecessor's layer of this kind forward only for a window that had settled when the predecessor was cut (`e + 2 ≤ sequence − 1`), and builds a younger window from its stores. A publish carries this kind only from the predecessor's manifest, never from an interrupted publish's resumption record, which does not say at which sequence it built a layer; a publish with no predecessor builds every window of this kind.

`first-mint-chunks` and `policy-first-mint-chunks` are period logs (decision 0049): API-only folds of history that ESTART seals per epoch once the epoch can no longer roll back. Epoch `e`'s rows key at `epoch_start(e)` and are written by the seal at the boundary that seals `e`, so the stele cut at that boundary is the first whose `e` window carries them; until then the epoch rides in the state tip as `state-first-mints`, one row per asset first minted in it. What tells a first mint from a re-mint is the archive's `asset` entity index for sealed epochs and these staged rows for unsealed ones, so no state namespace records it. A `Set` (decision 0057) lives in state only, one row per member keyed `group ‖ epoch ‖ member`, and its seal builds a `Chunk` log with one list per group and epoch: `first-mints` seals into both chunk logs, `first-scripts` into `first-script-chunks`, `account-addresses` into `account-address-chunks`, `asset-mints` into `asset-mint-chunks`, and `payment-addresses` with `payment-stats` into `payment-address-chunks`. Both first-mint chunk logs are lists in mint order, split into rows of at most 256 entries: `first-mint-chunks` holds the subjects an epoch first minted, keyed `chunk(4, BE)`, and `policy-first-mint-chunks` the names one policy first minted, keyed `policy(28) ‖ chunk(4, BE)`. Every row's field 0 is the list's total for the epoch. `first-script-chunks` is a list by the same rules: the script hashes an epoch saw first, in first-seen order, keyed `chunk(4, BE)`, each row `[count, hashes]`. Its `Set`, `first-scripts`, has an empty group, so its rows key `epoch(8) ‖ script hash(28)` and hold where the script was first seen, `[slot, tx_index, order]`: the block's slot, the transaction's index in the block, and the script's index in the transaction's script order. A transaction's script order is: its outputs' reference scripts, in output order; its collateral return's reference script; its witness scripts, native and Plutus merged, ascending by hash; its auxiliary-data native scripts, then its auxiliary-data Plutus V1 scripts, each in list order. A transaction that failed phase-2 validation contributes its collateral return's reference script alone. Auxiliary-data Plutus V2 and V3 scripts are not listed, because Pallas does not expose them. A script listed twice keeps its first position. The seal lists an epoch's scripts ascending by `(slot, tx_index, order, hash)`. What tells a new script from a repeat is the archive's `script` entity index for sealed epochs and these staged rows for unsealed ones, so no state namespace records it. `asset-mint-chunks` is a chunked list by the same rules: one asset's mints and burns by valid txs in an epoch, in chain order, keyed `AssetKey ‖ chunk(4, BE)`, each entry `[tx hash, 2 × amount + names_asset]`, a signed integer holding the amount, never zero, and in its low bit whether the tx's CIP-25 metadata (label 721) names the asset, which a burn never does; an unsealed epoch rides in `state-asset-mints`, one row per mint of the same shape keyed `AssetKey ‖ epoch ‖ slot ‖ tx index`. `account-address-chunks` is a period log by the same rules: one row per account an epoch saw an address under, keyed `AccountKey`, listing the epoch's addresses in order of first appearance within it. It is not chunked, because a reader removes repeats across epochs and so decodes every row it reaches. `address-stats`, `payment-address-chunks` and `account-stats` are period logs by the same rules (decision 0050). `address-stats` and `account-stats` are one type each in both stores (decision 0056), so their unsealed epochs ride in `state-address-stats` and `state-account-stats`, keyed `epoch ‖` the log key. An `address-stats` row holds one address's epoch as `[{unit: [received, sent]}, tx_count]`, the unit empty for lovelace and `policy ‖ name` for an asset, each sum a CBOR unsigned integer or, past u64, a positive bignum; a `payment-address-chunks` row holds `[tails, tx_count]`, the epoch's addresses of one payment credential without their payment hash, in byte order; an `account-stats` row holds `[tx_count]`, the distinct txs that moved value through any of the account's addresses, and exists for every epoch in which one did. Unsealed address and account rows are keyed epoch first, unlike every `Set` with a group, because an address has no fixed-width group to lead its key and an account's rows are found by point reads. The kind token is `log-` followed by the namespace with `_` rewritten to `-`, since a media type's kind token admits hyphens and not underscores; the mapping is injective, and a publisher spells the kinds out rather than composing them, so a namespace rename cannot silently rename a published kind. The namespace is therefore **not** in the record — it is the layer — and a restore writes a layer's records into the namespace its kind names. A publisher that finds logs under a namespace no kind covers **fails the publish** naming the namespace: the format has no layer to carry them, and shipping a snapshot that silently omits a slice of the ledger is the failure that costs most and shows least. Log layers wear the epoch scope unchanged, so per-(kind, scope) inheritance works across the split with no new scope shape.

**The seal.** ESTART opening epoch `b` seals every staged epoch `e` with `epoch_start(e + 1) + stability_window ≤ epoch_start(b)`, where `stability_window = ⌈3k/f⌉` slots, `k` the genesis security parameter and `f` the active slot coefficient: `b` is the first boundary past which no block of `e` can roll back. When epoch `e + 1` is at least a stability window long, that is `b = e + 2`; shorter epochs seal later. A 21,600-slot Byron epoch under a 129,600-slot window (k = 2160, f = 0.05) seals at `e + 7`. The period logs and the entity layers of `e` first appear in the stele cut at `b`, and never change after it.

Empty log and entity layers are omitted: **a `log-{ns}` or `entity-{kind}` layer exists if and only if it holds at least one record.** The rule is content-determined and not writer discretion — that is what keeps it deterministic across publishers, since two honest publishers agree about whether a layer exists exactly because they agree about whether a record does. Byron alone sheds ~1,200 empty blobs to it. Absence is normative and never a defect: a restore that finds no `log-pool-epochs` for an epoch restores that epoch with no pool epoch logs, exactly as the ledger had none. Every other kind keeps the arity it had — `blocks` and `indexes` are one layer per epoch whatever the window holds, and every state layer is always present, empty or not.

#### Value encoding

A record value — the `value` of a `log-{ns}` or `state-{ns}` record, `state-utxos` included — is one CBOR data item. The protocol carries it as opaque bytes, so SPEC.md's encoding profile does not reach inside it; this section fixes its encoding instead.

Integers, lengths and tag numbers are in shortest form, and every array, map and string has a definite length. Two tags occur, and no others:

- **tag 2**, an unsigned bignum: the value's big-endian bytes with no leading zero byte, used only for a value above 2^64 − 1. A smaller value is a plain unsigned integer. The `address-stats` sums are the one field that uses it.
- **tag 30**, a rational `[numerator, denominator]`: committee thresholds, pool margins, and the protocol-parameter intervals and prices.

No value holds a float.

A map's keys are in the order of its key type, the order the in-memory map iterates, not the encoded-key order of RFC 8949 §4.2.1. Each key occurs once.

- A map keyed by a stake credential lists script hashes before key hashes (`82 01 …` before `82 00 …`), each ascending by hash, because Pallas declares `ScriptHash` first. These are `gov`'s committee members, committee authorizations, dissolved committee's authorizations and DRep snapshot, and the committee and DRep votes of `proposals`.
- A map keyed by a DRep, `gov`'s DRep distribution, follows Pallas's declaration order: key hashes, then script hashes, each ascending by hash, then `Abstain`, then `NoConfidence`.
- A map keyed by a byte string, an `address-stats` row's units, ascends by the raw bytes, not by their encodings, so a longer unit can come before a shorter one.
- A map keyed by an integer or a fixed-width hash ascends: the unknown cost models, `gov`'s pool distribution, the pool votes of `proposals` and `epochs`' `estart_tally`.

A list's order is part of its value. These lists are sorted: a protocol-parameter set (`epochs`' parameters and a `ParamChange` action in `proposals`) lists its values ascending by kind, the variant index, each kind once; an `account-epochs` row lists `leader_rewards` and `deposit_refunds` ascending by pool, then amount; and a `payment-address-chunks` row lists its tails ascending by their raw bytes, not by their encodings. Other lists keep the order of the chain data they are built from: a pool's owners and relays, and the members an `UpdateCommittee` action removes, are in their transaction's order, an authorization history is in slot order, and the chunk logs keep the orders stated above. Two `proposals` lists come from maps in the transaction and keep the map's order: the members an `UpdateCommittee` action adds are credential-keyed, so script hashes come first as above, and a `TreasuryWithdrawal`'s entries ascend by reward-account bytes, so key hashes come before script hashes.

Decoders check none of these orders. A value written in another order is read, but its bytes, and the digest of its layer, differ from those of a publisher that follows this section.

Chain positions take several shapes, each fixed as tabled. A tuple is a CBOR array of its fields in order. `TxOrder`, a transaction's index in its block, is a Rust `usize` encoded as a shortest-form uint, and is always below 2^32, so it encodes exactly as a `u32` would.

| Shape | Encoding | Where |
|---|---|---|
| bare slot | `slot: uint` | `accounts.registered_at`, `accounts.deregistered_at`, `pruned-accounts.deregistered_at` |
| `(slot, TxOrder)` | `[slot, tx_index]` | `accounts.vote_delegated_at`; `dreps.registered_at`, `dreps.unregistered_at`; `gov`'s DRep snapshot `unregistered_at` |
| `VoteEntry` | `[newest: [slot, vote], standing: [slot, vote] / null, newest_order: uint, standing_order: uint]`, `vote` Pallas's `Vote` as a uint (0 No, 1 Yes, 2 Abstain), each order the `TxOrder` of its vote's transaction, `standing_order` 0 without a standing vote | `proposals.cc_votes`, `proposals.drep_votes` and `proposals.spo_votes` values |
| `CertPosition` | `[slot, tx_index, cert_index]`, the certificate's index among its transaction's | `drep-sightings.first_seen` |
| `MintPosition` | `slot(8, BE) ‖ tx_index(4, BE)` in the key | `asset-mints` keys |
| first-mint position | `slot`, `tx_index: u32` at fields 3 and 4 | `first-mints` values |
| `FirstScriptSet` | `[slot, tx_index: u32, order: u32]` | `first-scripts` values |
| account-address chain order | `slot`, `tx_index: u32`, `output_index: u32` at fields 3 to 5 | `account-addresses` values |
| `UtxoPosition` | `slot, tx_index` as the last two fields of the value, `tx_index` a `u16` | `utxos` values |

#### Compatibility contract

Within a media-type version, `.v{x}` is a **contract on record content, not an exact-byte pin** (decision 0026). Writers MAY append numbered optional or `#[cbor(default)]` fields; readers MUST skip fields they do not know; field indexes are never renumbered, removed or repurposed. Every append moves that namespace's `schemas` revision — additive included, because `schemas` is what keeps the inscription a full pin of the bytes rather than a compatibility gate. `v{x}` bumps are reserved for breaking changes.

Enums are the contract's hard edge and the one place the rule inverts. A minicbor enum refuses a variant index it has never heard of, so **adding a variant to any enum reachable from a record is reader-breaking within `v{x}`**, whatever the field policy says: it requires a media-type version bump on the kinds that carry it, or an explicit waiver recorded in this document (see "Known edges").

The contract is enforced by `crates/snapshot/tests/field_registry.rs` and its `tests/registry/` data module, which is where a record shape is actually pinned. Per namespace it holds a **canary** — a fully-populated value, every `Option` `Some` and every collection non-empty, except the `epochs` sync cursors, which a published row never carries — and the hex of its encoding through the production encode path, at that namespace's current `schemas` revision. The suite then asserts, on every build:

- the canary still encodes to its pinned bytes, so a renumbering, a removal or a silent width change is a build failure rather than a moved digest with no explanation;
- the registry's current revision equals `SCHEMA_REVS[ns]`, so a field appended without its revision bump — or a bump with nothing pinning it — fails in either direction;
- every **retained** revision still decodes under today's decoder, which is the reader tolerance the contract promises, asserted rather than assumed. Retained canaries are append-only: never edited, never deleted. A change that makes one undecodable is breaking by definition.
- every reachable enum's variant table is pinned per variant, including the Pallas enums the records embed — a dependency upgrade that renumbered `Relay` or `DRep` would otherwise change published bytes with nothing in this repository to notice;
- the codec's own tolerance behaviour (unknown trailing field skipped, index gap null-padded, missing trailing field defaulted) holds against the pinned `minicbor` version, so a codec upgrade that changed any of the three breaks in a test rather than in a published stele;
- the same value encodes to the same bytes across repeated construction. A failure there is never a re-pin: it is an encoding-determinism defect, and it breaks the cross-party digest identity this whole document rests on.

#### Known edges

The contract has edges it does not close. Each is waived here, by its finding number in the freeze-hardening review, and each is what a reader must allow for.

- **Hard-fork enums (7).** A hard fork that adds a variant to `PParamValue`, `ProposalAction`, `GovPurpose` or a Pallas enum reachable from a record bumps the media type of every kind that carries it. A fork that adds a protocol parameter bumps `state-epochs`, `log-epochs`, `state-proposals` and `log-proposals`; `state-gov` carries no parameter value and keeps its media type. Variants are only appended, so a newer reader still reads older bytes.
- **Pallas codecs (8).** A Pallas codec change that moves a record's bytes is a profile event. The variant tables and the per-struct canaries in `field_registry.rs` catch it as a build failure.
- **Enum order shared with the WAL (9).** `ProposalAction`'s variant order is also the WAL's bincode format. Reordering it costs a WAL version bump as well as the media-type bump.
- **Credential and key shapes (10).** The key kinds stay as tabled under "Key kinds", four credential encodings included.
- **Chain positions (11).** The position shapes stay as tabled under "Value encoding", with `TxOrder` below 2^32.
- **Values that repeat their key (13).** `accounts.credential`, `pending-rewards.credential` and `pending-mirs.credential`, `pools.operator`, `proposals.{tx, idx}`, `dreps.identifier`, and the group and epoch fields of every `Set` row repeat bytes of their key. They must agree with it, and a restore does not check that they do.
- **Names (14).** `epochs` and `payment-stats` keep their names under the naming grammar above.
- **Datum refcount (18).** `datums.refcount` follows the rule stated under `state-datums`, as it is.
- **Byte-string values.** A record value is a CBOR byte string to SPEC.md's frame validation, which does not look inside it; the value follows this document's value-encoding section instead.

The `digests` layer covers the immutable files fully contained in the stele's block range: `lastImmutable` is derived from the boundary slot and the chunk geometry observed in the chain — canonical, never dependent on aggregator state at publish time. Digest values equal Mithril Cardano DB v2's merkle leaves (hex-decoded), so any Mithril certificate whose beacon covers `lastImmutable` can verify them via the aggregator's digest route and a merkle proof. The certificate reference is deliberately *not* part of the inscription: certificates are produced on the aggregator's cadence, so two independent publishers at the same boundary would reference different certificates — including one would break cross-publisher determinism, while the digest values themselves are byte-stable properties of the chain.

### The cut point and the boundary sliver

A stele is cut by syncing with `dolos sync catchup --follow --stop-epoch E`, and the halt is **one block past the epoch boundary**, not on it. The sync crosses the boundary — Ewrap closes epoch E-1, Estart opens epoch E — and then applies the first block of epoch E before stopping. That block is what makes the stele addressable: Estart alone leaves the cursor a bare `ChainPoint::Slot` carrying no block hash (`estart::commit_finalize`), and `position.point` must carry one for a stele to be verifiable against a chain. The anchoring block may itself sit exactly on `epoch_start(E)`, in which case the sliver is one slot wide.

So the epoch windows a stele covers are `0..=E`: epochs `0..E-1` **complete**, plus epoch E's **boundary sliver** — the window that opens at `epoch_start(E)` and closes at the anchoring block. The sliver is normative, not an artifact of where a publisher happened to stop.

It is load-bearing because of how boundary data is keyed. Every log row keys at the start of an epoch, but not always the epoch its boundary opens or closes: each log kind's anchor is in the table below. ESTART opening E writes E's stake-only `account-epochs` rows at `epoch_start(E)`, so they live inside the sliver and nowhere else: a stele that dropped the sliver in the name of a clean boundary would ship a state tip whose opening rows are in no layer at all, and a restored node would never regenerate them.

Log anchors. For each log kind: what writes epoch `e`'s rows, the slot they key at, and the first stele whose `e` window carries them. A window's layer does not change after that stele, except `log-account-epochs`.

| Log | Written by | Key slot | First stele with window `e` |
|---|---|---|---|
| `epochs` | EWRAP closing `e` | `epoch_start(e)` | `e + 1` |
| `drep-epochs` | EWRAP closing `e`, the distribution it completes | `epoch_start(e)` | `e + 1` |
| `account-epochs` | ESTART opening `e` (stake leg alone), then EWRAP closing `e + 1` (full rows, same keys) | `epoch_start(e)` | `e` with the stake-only rows; settled from `e + 2` |
| `pool-epochs` | RUPD during `e + 1`, settled by EWRAP closing `e + 1` on what the boundary paid | `epoch_start(e)` | `e + 2` |
| `pools`, `dreps`, `proposals` (final rows) | ESTART opening `e + 1`, `e` the closed epoch | `epoch_start(e)` | `e + 1` |
| period logs (`*-chunks`, `account-stats`, `address-stats`) | the seal at boundary `b` (see "The seal") | `epoch_start(e)` | `b`; `e + 2` when epoch `e + 1` is at least a stability window long |

EWRAP closing `e` therefore also writes rows at `epoch_start(e - 1)`: the full `account-epochs` rows of `e - 1`, and the settled `pool-epochs` rows of `e - 1`.

`sequence`, the immutable tag's E, and `position.epoch` are consequently **one number**: the epoch the cursor stands in, which is also the last epoch the layers cover. The operator-facing form of the same rule is *stop at the epoch you want the node to start in* — `--stop-epoch E` produces the stele tagged `epoch-E`.

#### Only the boundary cut is published

**A stele captures the state immediately after the first block of its sequence epoch, and no other state.** The cut above is not one choice among several a publisher may make: every path that builds layers — directory and registry publication, a registry dry run, `dolos snapshot digest` and `verify --reproduce` — establishes it from the stores before a layer is written, and refuses otherwise. The check is `dolos_snapshot::boundary::check`, and the export itself runs it, so a library caller holding a hand-built `Plan` cannot reach the layers without it. There is no flag that skips it.

The evidence is the stores' own, never configuration and never arithmetic on the cursor's slot — a node can be configured for one epoch and stand in another, and an epoch's first block rarely sits on its first slot:

0. The archive starts at the chain's origin: its first block is the chain's first (block number at most 1, the test `doctor rebuild-state` applies). **A publisher keeps full history** (decision 0064). A pruning node's sweep removes the entity index entries of the history it drops, so it stages a re-carried script or asset again as a first sighting and seals it into its period logs; that is a pruned query node's accepted posture and exactly what a stele must never carry. The refusal names the archive's first block and slot.
1. The state store's cursor is the plan's point, hash included, and the plan's sequence is the epoch that point stands in.
2. The archive holds the block at the cursor, identified by its hash among the blocks at its slot.
3. The block before it in the archive, in chain order, is the block its header names as its parent.
4. That parent stands before `epoch_start(E)`.
5. The archive ends at the cursor's block.
6. The state's bookkeeping shows every boundary pass finished: no sync cursor is set on the `epochs` tip, and both `gov` distributions have every shard merged.

Steps 3 and 4 together prove the cursor is the epoch's first block: the parent link rules out anything between the two, however many empty slots separate them. A Byron epoch-boundary block, which shares its slot with the first main block after it, is the epoch's first block — its parent is the last main block of E-1 — while that main block is its second, whose parent is the boundary block in E.

Step 5 keeps the stele to the state. Export reads logs and indexes by slot alone, and blocks applied after the cursor write rows inside the stele's windows: Ewrap closing epoch E keys E's `EpochState` at `epoch_start(E)`, inside the sliver, and epoch rewards and period seals key rows at earlier epoch starts; a main block beside a Byron boundary block shares the cursor's own slot. A sync stopped at E stores nothing past E's first block. An archive that holds more ran on past a rebuilt state, such as `doctor rebuild-state --stop-epoch` over a synced archive, and it is refused.

Step 6 keeps crash-recovery bookkeeping out of the stele. At a finished boundary cut the sync cursors are clear and the shard counters equal, so a tip that shows otherwise is a pass still in flight, and the refusal names what is set.

An archive that cannot show step 3 is **insufficient evidence**, and is refused rather than trusted: one pruned past the parent, or one that begins at the cursor because the node was bootstrapped there. Step 0 already refuses both whenever the first block held is past the chain's first. A pruned archive's first remaining block says nothing about what came before it. The refusal names the cursor and the epoch, and says which store to publish from instead.

**Epoch 0 is never published.** No boundary opens it, so a stele at sequence 0 would follow no transition; the first stele of any chain is `epoch-1`.

**A node past the boundary cannot publish that epoch.** Its stores no longer hold the boundary state, and nothing reconstructs it. The refusal says so and points at the remedies: restore the `epoch-E` stele, or let the node run on to the next boundary and cut that one with `dolos sync catchup --follow --stop-epoch E+1`.

**The stores must be still for the duration.** The check reads them once, before the walk; it does not freeze them, and no cursor check could make an export over stores something else is writing to consistent. Stillness comes from how the stores are opened: the storage backends lock their directories, so no second process — a daemon included — writes to stores a `dolos snapshot` command holds, and an in-process host reads through the headless replay API, whose snapshot is a borrow that no import can run beside.

**Reading is unaffected.** The rule is a publisher eligibility check. A restore, `inspect`, and transport `verify` read any well-formed stele. Reproduction is the exception: `digest` and `verify --reproduce` only run over stores standing on a boundary.

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
  "parameters": { "entities": {"asset": 32, "script": 28, "stake": 29},
                   "indexKeyHash": "xxh3-64",
                   "shards": {"accounts": 16, "datums": 16, "utxos": 16, "…": 1},
                   "schemas": {"accounts": 1, "utxos": 1, "…": 1},
                   "stateEpochs": [208, 236, 290, 365] },
  "compression": {"algo": "zstd", "level": 9},
  "history": [
    {"sequence": 548, "inscriptionDigest": "sha256:…"},
    {"sequence": 549, "inscriptionDigest": "sha256:…"} ],
  "layers": [
    {"kind": "blocks", "mediaType": "application/vnd.dolos.stelae.blocks.v1+zstd",
     "diffId": "sha256:…", "records": 21600, "uncompressedSize": 43210000,
     "scope": {"epoch": 0, "startSlot": 0, "endSlot": 21599}},
    {"kind": "state-utxos", "mediaType": "application/vnd.dolos.stelae.state-utxos.v1+zstd",
     "diffId": "sha256:…", "records": 812345, "uncompressedSize": 402653184,
     "scope": {"shard": 0}},
    {"kind": "state-pools", "mediaType": "application/vnd.dolos.stelae.state-pools.v1+zstd",
     "diffId": "sha256:…", "records": 3210, "uncompressedSize": 1048576,
     "scope": {"shard": 0}},
    {"kind": "state-pools", "mediaType": "application/vnd.dolos.stelae.state-pools.v1+zstd",
     "diffId": "sha256:…", "records": 2980, "uncompressedSize": 972800,
     "scope": {"epoch": 365, "shard": 0}},
    {"kind": "digests", "mediaType": "application/vnd.dolos.stelae.digests.v1+zstd",
     "diffId": "sha256:…", "records": 6188, "uncompressedSize": 618800,
     "scope": {"lastImmutable": 6187}} ] }
```

`parameters` is the profile's compatibility declaration. Every value but one is a consequence of publisher code rather than a free choice: `entities` declares the entity kinds and their id widths (`ENTITY_KINDS`); `indexKeyHash` names the hash behind the pre-hashed index keys; `shards` is the per-namespace shard count map above, each namespace splitting on the shard byte the state-kinds rule fixes; and `schemas` is a per-namespace revision of the *record content* — the stored value a `state-{ns}` or `log-{ns}` layer carries verbatim — which moves when that namespace's stored shape changes, plus one entry at revision `0` per retired namespace, per the removed-kind rule above. `SCHEMA_REVS` in `crates/snapshot/src/namespaces.rs` holds each live namespace's current revision, and the namespace's entry in `crates/snapshot/tests/registry/mod.rs` records why it moved. Every live revision is pinned by a canary in `crates/snapshot/tests/field_registry.rs`, which fails the build when a record's field table moves without its revision, or the other way round. The split between the two is deliberate: a change to how a layer is *framed* moves that kind's media type and fails closed at the transport, while a change to what a record *contains* moves its schema revision, which a reader consults to decide whether it can interpret what it can already parse. The exception, `stateEpochs`, proves the rule: it is the publisher's configured retained set, and it is here precisely *because* it is a choice — declaring it is what turns a configuration difference between two publishers into a visible parameters difference instead of a silently divergent history.

`sequence` is the protocol's ordering key; this profile sets it to the epoch. The three opaque objects are canonicalized by JCS like every generic key (SPEC.md), so determinism holds without the protocol interpreting them — which is why every value in them must itself be deterministic, the property the compatibility contract above enforces.

### The manifest size arithmetic

The 4 MiB ceiling and its measurement are the protocol's (SPEC.md, "The
manifest size ceiling"); what is profile-owned is the arithmetic that keeps a Dolos stele inside the
ceiling, and it stays here: a descriptor with its annotations costs ~350
bytes, so the ceiling falls near 12,000 layers. A stele is bounded above by
its epochs × the epoch kinds (`blocks`, `indexes`, one `log-{ns}` per log
namespace and one `entity-{kind}` per entity kind), plus one state tip's worth of layers (16 per sixteen-way
namespace, 1 per single-blob namespace) for the tip and again for every
retained state dump; `EPOCH_KINDS` and `STATE_KINDS` give the current terms.
For mainnet at 20 retained epochs, the ceiling of what a publisher is
expected to configure, that bound sits well inside the ceiling, and it is
loose in the direction that helps: log and entity layers are omitted when
empty, and early eras carry few kinds of log and no entity of any kind at
all — mainnet's `asset` and `script` layers add 793 descriptors to epoch 646,
and its `stake` layers at most one per window from Shelley's first epoch, 208. **This is the arithmetic that
bounds the retained list**: a dump per epoch would put the state layers
alone at several times the ceiling.

### Publisher pipeline

1. Restore the publisher node from the previous stele (self-hosting delta pull; first run via Mithril). The node keeps full history: a publish from an archive that does not start at origin is refused.
2. Sync with `dolos sync catchup --follow --stop-epoch E` until `StopEpochReached` — the state crosses the boundary and lands on the **first block of epoch E**, the block that gives `position.point` a hash. The publish establishes that from the stores before it writes a layer (see "Only the boundary cut is published").
3. The Stelae Cardano publisher uploads only the newly closed epoch's layers, epoch E's boundary sliver, the `log-account-epochs` layer of epoch E-2, which settled at this boundary, the `log-pool-epochs` layer of epoch E-2, which RUPD wrote during E-1, and the layers of every epoch the seal at this boundary sealed (its period logs and `entity-{kind}` layers; see the log anchors table); fresh state layers + inscription; tag `epoch-E`, move `latest`. On networks with a Mithril aggregator, fetch the immutable-file digest list from the aggregator's digest route, verify it against a certificate, and write the `digests` layer for the files within the boundary.
4. Determinism job: an independent runner that synced to the same boundary by any means runs `dolos snapshot digest` and alerts on inscription mismatch.
5. Matching verifiers sign and push referrer signatures; clients enforce k-of-n.

Registry hygiene: keep a trailing window of `epoch-E` tags (e.g. 12); untagged state blobs are reclaimed by registry GC; epoch blobs remain referenced by later manifests. Trust evidence for reclaimed steles survives in the `history` of every later inscription.

### Restore pipeline

1. Resolve tag → manifest → inscription; verify its digest, schema, profile name and major version, network magic and signatures.
2. Plan which layers to consume — no kind is mandatory: ledger-only nodes skip `blocks`/`indexes`/`log-{ns}`/`entity-{kind}`, and a layer of a kind this client does not implement is skipped too, reported alongside the epochs a rolling `history.window` drops — unless its `scope` marks it `required`, which refuses the restore before a store is opened; a future Mithril-sourced mode fetches block data from an aggregator instead of `blocks` layers, verifying each immutable file against the `digests` layer before the usual decode→append import. A stele that does not declare, in `parameters.entities`, every entity kind this client registers, or declares one at another width, is refused here too. Plan the epoch range from `history.window`; diff against the progress file (`<storage.path>/.snapshot-restore.json`, records inscription digest + completed layer diffIds) for `--continue`. Preflight: sum the `uncompressedSize` of the planned layers and fail early if free space at `storage.path` is insufficient; derive download progress and time-remaining estimates from the compressed blob sizes of the layers that remain to be fetched — excluding layers already completed per the progress file or already present locally — so resumed and deduplicated restores report correct totals.
3. Open stores.
4. Per epoch (checkpointed): fetch + verify `blocks`, each `log-{ns}` and each `entity-{kind}` the epoch carries → archive appends and entity index writes, commit; fetch `indexes` → pre-hashed appends, commit. A restore planned with a history window takes the entity layers of its window only, which is what a pruned node keeps.
5. State tip: fetch every shard of every `state-{ns}` kind (parallelizable) → dispatch on the kind; commit. For each incoming UTxO chunk, derive the live-UTxO tags through `utxo_index_delta_from_utxo_delta` and apply both the UTxO set and tags through the same state writer before committing. Do not hold a state-store iterator open across these writes: its database snapshot can retain flushed memtables for the whole restore. State-tip layers are always replayed on resume.
6. `set_cursor(position.point)` as the last write of the restore, so `has_existing_data()` only ever sees a node whose live-UTxO dimensions are there too. The cursor is the completion marker for the whole restore rather than for the state tip alone: an interruption anywhere leaves a node that reports as empty, which is what it is, and `--continue` repairs it from the epoch checkpoints.
7. Delete progress file; existing `seed_wal_from_state` reseeds the WAL; the daemon chain-syncs the partial current epoch.

Steps 1–2 and the fetch/verify half of steps 4–5 are protocol code; the store writes are profile code.
