//! Done criterion 3: the golden digests.
//!
//! Every value pinned here is a sha256 over bytes this profile and ADR-004
//! together fully determine — the deterministic CBOR sequence of each layer,
//! and the RFC 8785 canonical JSON of the inscription. Nothing platform-,
//! timing- or compression-dependent enters them, so they hold across machines
//! and across zstd versions.
//!
//! That makes this file the drift alarm for the whole profile. These digests
//! *are* published identity: a change to a record shape, a scope key spelling,
//! a media type, a namespace string or an exact-kind literal moves one, and
//! moving one silently is precisely the failure Stelae exists to prevent. A
//! deliberate format change updates these in the same commit that changes the
//! ADR. An accidental one shows up here first — and a value that disagrees with
//! a re-run on unchanged code is encoding nondeterminism, which is a finding,
//! never a re-pin.
//!
//! Between them these freeze: every media type, the tag string, every archive
//! dimension name and exact-record kind literal, every `log-{ns}`,
//! `entity-{kind}` and `state-{ns}` kind string — which is where the namespace
//! and kind strings live now that no record names one — the layer header and
//! scope shapes, and the `position`/`parameters` key spellings, the shard,
//! schema and entity maps included.

mod common;

use common::*;
use dolos_core::{BlockHash, ChainPoint};
use dolos_snapshot::{
    entity_kind_for,
    layers::{blocks, entities, indexes, logs, state},
    log_ns_for, state_ns_for, DolosProfile, BLOCKS, INDEXES,
};
use stelae::{
    dir::SnapshotDir,
    frame::Limits,
    inscription::{canonical_json, HistoryEntry, Inscription},
    Digest, SnapshotReader, SnapshotWriter,
};

/// `(kind, diffId, records, uncompressedSize)`, in inscription order.
///
/// `records` counts the protocol's header record, as a descriptor does.
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 45] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:fb039df612425645ba18b366a3625d4c41e2f1a8fb551b0609b131284ae3bda0",
        23,
        619,
    ),
    (
        "log-account-address-epochs",
        "sha256:d2bb13a245ed90481816d2d1b8d4e72b9e3cdfd7b0d784c8c8e49a58e2f8934f",
        2,
        115,
    ),
    (
        "log-account-epochs",
        "sha256:9cfebfa5dd85cadd44e59d0e51b26c3adeca347f0aea30188d96dbf1eeb22be7",
        2,
        102,
    ),
    (
        "log-account-stats",
        "sha256:e055e50725da443014dab37602e16a8b40bbefc64183ac7a9e77159ec952143f",
        2,
        101,
    ),
    (
        "log-address-stats",
        "sha256:de7b9f85d5fc130fb4ed732da9448572233907f4966e5ac435530669239c87aa",
        2,
        130,
    ),
    (
        "log-asset-mint-chunks",
        "sha256:c87071e2fbe70f006cf0100e0577a306831909d58c51426f7ecffa786e4a500b",
        2,
        109,
    ),
    (
        "log-drep-epochs",
        "sha256:d649d6843908e3cb650b38066dd5b061d4adbe74cf17fe1a3c5498d43b196b67",
        2,
        96,
    ),
    (
        "log-dreps",
        "sha256:9c0dff7fc49b26035e4f847167a57825e01a97d0c7e580375e4c7c9f5a2692e4",
        2,
        90,
    ),
    (
        "log-epochs",
        "sha256:dfdbf6b422298a6d2fa090e36dfc5f48dd3527bfc6d7f3ee678644ec094a38c6",
        2,
        61,
    ),
    (
        "log-first-mint-chunks",
        "sha256:1e041ead8be23e9455adfe9a3b7f40bd304c63cc499e1929ecfb1cffb5952e92",
        2,
        76,
    ),
    (
        "log-first-script-chunks",
        "sha256:d54c77d975358c4e4461b1e5eb44f63fe2e474a77ad93601048cc41df1aaa8e0",
        2,
        78,
    ),
    (
        "log-payment-address-epochs",
        "sha256:7206d9869b233e42c4d689ca704071c6b3958662e75502875550f9a6af7d2316",
        2,
        111,
    ),
    (
        "log-policy-first-mint-chunks",
        "sha256:99fe7d4fbdcb0a580982e861ddf6466c4b40d5cdd8a812c596296c32ff0fa1b0",
        2,
        113,
    ),
    (
        "log-pool-epochs",
        "sha256:586dc90b6000f08622a694aefefbcd9674aea59db02f94966fd2f7845bcc25ff",
        2,
        95,
    ),
    (
        "log-pools",
        "sha256:b505113d38d258dcbd86078c80c7c7c0c080d3f9b542148f3150b5f3faa5bec9",
        2,
        89,
    ),
    (
        "log-proposals",
        "sha256:e4b121c8ea094b9e613bc8c38c0151289612456b454376e4722daa4091b39778",
        2,
        101,
    ),
    (
        "entity-asset",
        "sha256:b820744b499226137d7521dce605dae7c0f17fb1964dd5b8967d0ec2669a233c",
        3,
        124,
    ),
    (
        "entity-script",
        "sha256:ca4862a274c299824946a599d5d5468bd4944dbfabd396b767e83798ae5aae2b",
        3,
        117,
    ),
    (
        "entity-stake",
        "sha256:26e58433f499d63f4d1cc0a6d007a3a3e699a8d4adc5009f9a8d419752f9a6e3",
        3,
        118,
    ),
    (
        "state-account-addresses",
        "sha256:5296f2db380326b740501e5706a980bf5dc8f96d8fc6b5c3e37f825ebc43b204",
        2,
        165,
    ),
    (
        "state-account-history",
        "sha256:b209613f115dddc3b4640598614219f7134454c7dea863d4648e89f21beca75a",
        2,
        94,
    ),
    (
        "state-account-stats",
        "sha256:4b975663bbfa3cc3ace23747798006f9c4522b23ada4ab24e915b55669a1a7e2",
        2,
        100,
    ),
    (
        "state-accounts",
        "sha256:8a7f1a94728c2ca976c3b707e5e58836f7496247c6544f6f5ea0c3ef10f8a16a",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:cc80b288bae52273c8e3add97f66b64d2a3c3473c8c11c1c03d263481e665c78",
        2,
        87,
    ),
    (
        "state-address-stats",
        "sha256:0f6efee560dae76bc7b2606ad0976393e272eb95209b6510cf991747d7d2e14f",
        2,
        129,
    ),
    (
        "state-asset-mints",
        "sha256:abb614124da91e880ecad69f1328cfcbf42365361329deff228b53079a794eb1",
        2,
        110,
    ),
    (
        "state-datums",
        "sha256:8f9643ce925ad592e62c2f983a08c428f28fb29bb51933366bc6bba836a02a83",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:787590170393d2b876f4f3fba2cb0640462ce3dd94cba7825c90534e95e77e55",
        2,
        85,
    ),
    (
        "state-drep-observations",
        "sha256:58537af4c146e79a3ada8e8caffa0470e40f284a9e3e9b023e8a23e6404073e0",
        2,
        93,
    ),
    (
        "state-dreps",
        "sha256:65344d61b41dbba4434cb79c6b6bdbb5043bd01b6b4c7486f080d944cc60029a",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:46ccba5a377265c232d395c5f1743992eb157dfc7993a0a8ec2c6845cb46fbf7",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:1428f74ac30be5b84f17937afb1f41cbf30c4677698b7c3686afee195f696da3",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:04ef916b6323f686282d603a8edf0f988035390352002e07963f0e19030e96a4",
        2,
        126,
    ),
    (
        "state-first-scripts",
        "sha256:10ff2257802ddb96381112aa569bac0c82fdb26f323468aeec5ffb9514ee1391",
        2,
        96,
    ),
    (
        "state-gov",
        "sha256:c1614f8eeeda9390316ecb7fe40527a4c84e8eec01ee54a71dce70bb443b1c23",
        2,
        49,
    ),
    (
        "state-metadata-labels",
        "sha256:7e8bfb405b282d3ff72671b7e1262d23337c3ceb4d97abc78c3be26788ed1ca9",
        2,
        69,
    ),
    (
        "state-payment-addresses",
        "sha256:e8dc2798d9e16d7d7cf951ecad06f272c051c989f00a45d76d581611ce73769a",
        2,
        129,
    ),
    (
        "state-payment-stats",
        "sha256:f1e14f31961be425a1699e499a9295767a2d9265c5c2afc0830e70b1845d5508",
        2,
        96,
    ),
    (
        "state-pending-mirs",
        "sha256:d679db631912bbcc0e3677a8030d03e500cd431095c127892b21677c66d056d5",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:39fa85f5115b7e8ef7a2a839c3e06864d6751f61a647db46074da658d8a13e0c",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:2e08f9652e630bb56ccebab9fa7b9a718803b001a1e701daee0fc2f98386447d",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:b2b0cd4830b6717c2560c00186b083b16cf9aab342528d3382a575aac1c83859",
        2,
        92,
    ),
    (
        "state-utxos",
        "sha256:61d7398a24f1c458b5030b44c5677229cef133ffb47c9b6416fecaaacb5adcd6",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:f193e457cf6094ca38250b65ce07d37be4e056b816106a56545d751d30bd8673",
        2,
        91,
    ),
];

/// The stele's identity: sha256 of the canonical inscription.
const GOLDEN_INSCRIPTION: &str =
    "sha256:d1f31cce5cef5d28659630d9359daadf7aa5b529ad69ebbd91e1592f96273b23";

fn history() -> Vec<HistoryEntry> {
    vec![
        HistoryEntry {
            sequence: EPOCH - 2,
            inscription_digest: Digest::from_bytes([0x55; 32]),
        },
        HistoryEntry {
            sequence: EPOCH - 1,
            inscription_digest: Digest::from_bytes([0x66; 32]),
        },
    ]
}

/// The blocks snapshot a fixture stele names in `position.blocks`.
const BLOCKS_INSCRIPTION: [u8; 32] = [0x77; 32];

/// The identity of the same stele when its `position` names
/// [`BLOCKS_INSCRIPTION`].
const GOLDEN_INSCRIPTION_WITH_BLOCKS: &str =
    "sha256:81eb9054f4ce01feab57dc2b30e9993d7a260edeea73ca88f23729d686a8f8bf";

/// Write the whole fixture stele into `root`: its layers and an inscription.
fn write_stele(root: &std::path::Path) -> (Inscription, Digest) {
    write_stele_naming(root, None)
}

/// [`write_stele`], with `blocks` in its `position`.
fn write_stele_naming(root: &std::path::Path, blocks: Option<&Digest>) -> (Inscription, Digest) {
    let stele = SnapshotDir::create(root).unwrap();

    let point = ChainPoint::Specific(END_SLOT, BlockHash::new(POINT_HASH));

    let mut inscription = Inscription::new(
        &DolosProfile,
        EPOCH,
        dolos_snapshot::position(&network(), &point, EPOCH, blocks).unwrap(),
        dolos_snapshot::parameters(),
        dolos_snapshot::compression(),
    );

    inscription.history = history();

    inscription.layers = all_layers()
        .into_iter()
        .map(|(kind, scope, records)| {
            write_layer(&stele, kind, scope.as_ref(), &records).descriptor
        })
        .collect();

    let digest = stele.seal(&DolosProfile, &inscription).unwrap();

    (inscription, digest)
}

/// Each layer's identity, computed from the byte string a `diffId` covers,
/// without a directory in the way.
#[test]
fn per_kind_diff_ids_are_pinned() {
    let layers = all_layers();

    // Before the zip, not after: `zip` stops at the shorter side, so a dropped
    // layer would quietly shorten the loop rather than fail it — in the one test
    // whose whole job is to notice a layer changing.
    assert_eq!(
        layers.len(),
        GOLDEN_LAYERS.len(),
        "the stele no longer has the layers the goldens pin"
    );

    for ((kind, scope, records), (expected_kind, diff_id, count, size)) in
        layers.into_iter().zip(GOLDEN_LAYERS)
    {
        assert_eq!(kind, expected_kind);

        let bytes = sequence(kind, scope.as_ref(), &records);

        assert_eq!(
            Digest::compute(&bytes).to_string(),
            diff_id,
            "{kind}: diffId drifted"
        );
        assert_eq!(records.len() as u64 + 1, count, "{kind}: record count");
        assert_eq!(bytes.len() as u64, size, "{kind}: uncompressed size");
    }
}

/// The whole stele: written, read back through the streaming reader under the
/// default limits, and reproduced byte for byte by a second, independent write.
#[test]
fn a_complete_stele_reads_back_and_reproduces_its_digest() {
    let first = tempfile::tempdir().unwrap();
    let (inscription, digest) = write_stele(first.path());

    assert_eq!(
        digest.to_string(),
        GOLDEN_INSCRIPTION,
        "inscription digest drifted"
    );

    // The document survives the trip to disk in canonical form, and belongs to
    // this profile.
    let stele = SnapshotDir::open(first.path()).unwrap();
    let read = stele.read_inscription().unwrap();

    assert_eq!(read, inscription);
    assert_eq!(read.digest().unwrap(), digest);
    read.check_profile(&DolosProfile).unwrap();

    // Every layer streams back under the *default* record ceiling — the
    // confirmation the stelae crate's streaming reader was left waiting for from
    // its first real profile.
    let index = stele.blob_index().unwrap();
    assert_eq!(index.len(), GOLDEN_LAYERS.len());

    for descriptor in &read.layers {
        let mut reader = stele
            .stream_layer(&index, &DolosProfile, descriptor, Limits::default())
            .unwrap();

        assert_eq!(reader.header().profile, dolos_snapshot::PROFILE_NAME);
        assert_eq!(reader.header().kind, descriptor.kind);

        let mut records = 1u64;
        while let Some(record) = reader.next_record() {
            let record = record.unwrap();

            // Decoded, not merely counted: a record that frames cleanly and
            // does not decode is a layer this profile cannot restore.
            decode_one(&descriptor.kind, record);
            records += 1;
        }

        // Only now is the layer proven; everything above was read on the
        // strength of the descriptor.
        let digests = reader.finish().unwrap();

        assert_eq!(digests.diff_id, descriptor.diff_id, "{}", descriptor.kind);
        assert_eq!(records, descriptor.records, "{}", descriptor.kind);
    }

    // Written twice, independently: same document, same bytes, same blobs. This
    // is the property the whole protocol rests on, now under a real profile.
    let second = tempfile::tempdir().unwrap();
    let (again, again_digest) = write_stele(second.path());

    assert_eq!(again, inscription);
    assert_eq!(again_digest, digest);
    assert_eq!(
        std::fs::read(first.path().join("inscription.json")).unwrap(),
        std::fs::read(second.path().join("inscription.json")).unwrap(),
    );

    let second_index = SnapshotDir::open(second.path())
        .unwrap()
        .blob_index()
        .unwrap();
    for descriptor in &inscription.layers {
        assert_eq!(
            index.blob_for(&descriptor.diff_id),
            second_index.blob_for(&descriptor.diff_id),
            "layer {:?}",
            descriptor.kind,
        );
    }
}

/// The descriptors a `SnapshotDir` writes are the numbers the per-kind goldens
/// pin — so the two tests cannot drift apart and quietly agree with themselves.
#[test]
fn descriptors_match_the_per_kind_goldens() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    assert_eq!(
        inscription.layers.len(),
        GOLDEN_LAYERS.len(),
        "the stele no longer has the layers the goldens pin"
    );

    for (descriptor, (kind, diff_id, records, size)) in inscription.layers.iter().zip(GOLDEN_LAYERS)
    {
        assert_eq!(descriptor.kind, kind);
        assert_eq!(descriptor.diff_id.to_string(), diff_id);
        assert_eq!(descriptor.records, records);
        assert_eq!(descriptor.uncompressed_size, size);
    }
}

/// The canonical document itself, so a change to a key spelling, a media type
/// or an ordering shows up in the diff as text rather than only as a moved
/// hash.
#[test]
fn the_canonical_inscription_is_pinned() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();

    assert_eq!(canonical, CANONICAL_INSCRIPTION);

    // The vendor slot is ours and the protocol's reserved one is nowhere near a
    // payload type.
    assert!(canonical.contains("application/vnd.dolos.stelae.blocks.v1+zstd"));
    assert!(!canonical.contains("vnd.stelae.stelae"));
}

/// `position.blocks` (decision 0080): its spelling and form, and that a
/// position without it is the one the inscription golden above already pins.
#[test]
fn the_position_is_pinned_with_and_without_its_blocks_reference() {
    let point = ChainPoint::Specific(END_SLOT, BlockHash::new(POINT_HASH));
    let blocks = Digest::from_bytes(BLOCKS_INSCRIPTION);

    let canonical = |blocks: Option<&Digest>| {
        let position = dolos_snapshot::position(&network(), &point, EPOCH, blocks).unwrap();
        String::from_utf8(canonical_json(&position).unwrap()).unwrap()
    };

    assert_eq!(canonical(None), CANONICAL_POSITION);
    assert_eq!(canonical(Some(&blocks)), CANONICAL_POSITION_WITH_BLOCKS);
    assert!(CANONICAL_INSCRIPTION.contains(&format!(r#""position":{CANONICAL_POSITION}"#)));
}

/// The stele that names its blocks snapshot, end to end: the field moves the
/// identity, and the document reads back with it.
#[test]
fn a_stele_naming_its_blocks_is_pinned() {
    let temp = tempfile::tempdir().unwrap();
    let blocks = Digest::from_bytes(BLOCKS_INSCRIPTION);
    let (inscription, digest) = write_stele_naming(temp.path(), Some(&blocks));

    assert_eq!(digest.to_string(), GOLDEN_INSCRIPTION_WITH_BLOCKS);

    let read = SnapshotDir::open(temp.path())
        .unwrap()
        .read_inscription()
        .unwrap();
    assert_eq!(read, inscription);

    let position = dolos_snapshot::read_position(&read.position).unwrap();
    assert_eq!(position.blocks, Some(blocks));
}

const CANONICAL_POSITION: &str = concat!(
    r#"{"epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}}"#,
);

const CANONICAL_POSITION_WITH_BLOCKS: &str = concat!(
    r#"{"blocks":"sha256:7777777777777777777777777777777777777777777777777777777777777777","#,
    r#""epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}}"#,
);

/// The whole document, so the diff of a format change is readable.
///
/// JCS sorts object keys, so the layout below is the protocol's, not this
/// crate's — but every *string* in it is this profile's, and that is what the
/// literal is for.
const CANONICAL_INSCRIPTION: &str = concat!(
    r#"{"compression":{"algo":"zstd","level":9},"history":[{"inscriptionDigest":"sha256:5555555555555555555555555555555555555555555555555555555555555555","sequence":5},{"inscriptionDigest":"sha256:6666666666666666666666666666666666666666666666666666666666666666","sequence":6}],"layers":["#,
    r#"{"diffId":"sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106","kind":"blocks","#,
    r#""mediaType":"application/vnd.dolos.stelae.blocks.v1+zstd","records":4,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":167},"#,
    r#"{"diffId":"sha256:fb039df612425645ba18b366a3625d4c41e2f1a8fb551b0609b131284ae3bda0","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stelae.indexes.v1+zstd","records":23,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":619},"#,
    r#"{"diffId":"sha256:d2bb13a245ed90481816d2d1b8d4e72b9e3cdfd7b0d784c8c8e49a58e2f8934f","kind":"log-account-address-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-account-address-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":115},"#,
    r#"{"diffId":"sha256:9cfebfa5dd85cadd44e59d0e51b26c3adeca347f0aea30188d96dbf1eeb22be7","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:e055e50725da443014dab37602e16a8b40bbefc64183ac7a9e77159ec952143f","kind":"log-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":101},"#,
    r#"{"diffId":"sha256:de7b9f85d5fc130fb4ed732da9448572233907f4966e5ac435530669239c87aa","kind":"log-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":130},"#,
    r#"{"diffId":"sha256:c87071e2fbe70f006cf0100e0577a306831909d58c51426f7ecffa786e4a500b","kind":"log-asset-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-asset-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":109},"#,
    r#"{"diffId":"sha256:d649d6843908e3cb650b38066dd5b061d4adbe74cf17fe1a3c5498d43b196b67","kind":"log-drep-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-drep-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:9c0dff7fc49b26035e4f847167a57825e01a97d0c7e580375e4c7c9f5a2692e4","kind":"log-dreps","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-dreps.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:dfdbf6b422298a6d2fa090e36dfc5f48dd3527bfc6d7f3ee678644ec094a38c6","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:1e041ead8be23e9455adfe9a3b7f40bd304c63cc499e1929ecfb1cffb5952e92","kind":"log-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":76},"#,
    r#"{"diffId":"sha256:d54c77d975358c4e4461b1e5eb44f63fe2e474a77ad93601048cc41df1aaa8e0","kind":"log-first-script-chunks","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-first-script-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":78},"#,
    r#"{"diffId":"sha256:7206d9869b233e42c4d689ca704071c6b3958662e75502875550f9a6af7d2316","kind":"log-payment-address-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-payment-address-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":111},"#,
    r#"{"diffId":"sha256:99fe7d4fbdcb0a580982e861ddf6466c4b40d5cdd8a812c596296c32ff0fa1b0","kind":"log-policy-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-policy-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":113},"#,
    r#"{"diffId":"sha256:586dc90b6000f08622a694aefefbcd9674aea59db02f94966fd2f7845bcc25ff","kind":"log-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":95},"#,
    r#"{"diffId":"sha256:b505113d38d258dcbd86078c80c7c7c0c080d3f9b542148f3150b5f3faa5bec9","kind":"log-pools","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-pools.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":89},"#,
    r#"{"diffId":"sha256:e4b121c8ea094b9e613bc8c38c0151289612456b454376e4722daa4091b39778","kind":"log-proposals","#,
    r#""mediaType":"application/vnd.dolos.stelae.log-proposals.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":101},"#,
    r#"{"diffId":"sha256:b820744b499226137d7521dce605dae7c0f17fb1964dd5b8967d0ec2669a233c","kind":"entity-asset","#,
    r#""mediaType":"application/vnd.dolos.stelae.entity-asset.v1+zstd","records":3,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":124},"#,
    r#"{"diffId":"sha256:ca4862a274c299824946a599d5d5468bd4944dbfabd396b767e83798ae5aae2b","kind":"entity-script","#,
    r#""mediaType":"application/vnd.dolos.stelae.entity-script.v1+zstd","records":3,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":117},"#,
    r#"{"diffId":"sha256:26e58433f499d63f4d1cc0a6d007a3a3e699a8d4adc5009f9a8d419752f9a6e3","kind":"entity-stake","#,
    r#""mediaType":"application/vnd.dolos.stelae.entity-stake.v1+zstd","records":3,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":118},"#,
    r#"{"diffId":"sha256:5296f2db380326b740501e5706a980bf5dc8f96d8fc6b5c3e37f825ebc43b204","kind":"state-account-addresses","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-account-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":165},"#,
    r#"{"diffId":"sha256:b209613f115dddc3b4640598614219f7134454c7dea863d4648e89f21beca75a","kind":"state-account-history","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-account-history.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:4b975663bbfa3cc3ace23747798006f9c4522b23ada4ab24e915b55669a1a7e2","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:8a7f1a94728c2ca976c3b707e5e58836f7496247c6544f6f5ea0c3ef10f8a16a","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:cc80b288bae52273c8e3add97f66b64d2a3c3473c8c11c1c03d263481e665c78","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:0f6efee560dae76bc7b2606ad0976393e272eb95209b6510cf991747d7d2e14f","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:abb614124da91e880ecad69f1328cfcbf42365361329deff228b53079a794eb1","kind":"state-asset-mints","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-asset-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":110},"#,
    r#"{"diffId":"sha256:8f9643ce925ad592e62c2f983a08c428f28fb29bb51933366bc6bba836a02a83","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:787590170393d2b876f4f3fba2cb0640462ce3dd94cba7825c90534e95e77e55","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:58537af4c146e79a3ada8e8caffa0470e40f284a9e3e9b023e8a23e6404073e0","kind":"state-drep-observations","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-drep-observations.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:65344d61b41dbba4434cb79c6b6bdbb5043bd01b6b4c7486f080d944cc60029a","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:46ccba5a377265c232d395c5f1743992eb157dfc7993a0a8ec2c6845cb46fbf7","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:1428f74ac30be5b84f17937afb1f41cbf30c4677698b7c3686afee195f696da3","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:04ef916b6323f686282d603a8edf0f988035390352002e07963f0e19030e96a4","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:10ff2257802ddb96381112aa569bac0c82fdb26f323468aeec5ffb9514ee1391","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:c1614f8eeeda9390316ecb7fe40527a4c84e8eec01ee54a71dce70bb443b1c23","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:7e8bfb405b282d3ff72671b7e1262d23337c3ceb4d97abc78c3be26788ed1ca9","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:e8dc2798d9e16d7d7cf951ecad06f272c051c989f00a45d76d581611ce73769a","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:f1e14f31961be425a1699e499a9295767a2d9265c5c2afc0830e70b1845d5508","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:d679db631912bbcc0e3677a8030d03e500cd431095c127892b21677c66d056d5","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:39fa85f5115b7e8ef7a2a839c3e06864d6751f61a647db46074da658d8a13e0c","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:2e08f9652e630bb56ccebab9fa7b9a718803b001a1e701daee0fc2f98386447d","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:b2b0cd4830b6717c2560c00186b083b16cf9aab342528d3382a575aac1c83859","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:61d7398a24f1c458b5030b44c5677229cef133ffb47c9b6416fecaaacb5adcd6","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:f193e457cf6094ca38250b65ce07d37be4e056b816106a56545d751d30bd8673","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91}"#,
    r#"],"parameters":{"#,
    r#""entities":{"asset":32,"script":28,"stake":29},"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-epochs":1,"account-addresses":1,"account-epochs":1,"account-history":1,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"datums":1,"drep-epochs":1,"drep-observations":1,"dreps":1,"epochs":1,"eras":1,"first-mint-chunks":1,"first-mints":1,"first-script-chunks":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-address-epochs":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-first-mint-chunks":1,"pool-epochs":1,"pools":1,"proposals":1,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-history":1,"account-stats":1,"accounts":16,"address-stats":1,"asset-mints":1,"datums":16,"drep-observations":1,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"utxos":16}"#,
    r#"},"position":{"#,
    r#""epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":7}"#,
);
/// Decode one record with the codec its layer kind names, so the streaming pass
/// exercises every codec rather than only the framing underneath them.
fn decode_one(kind: &str, record: &[u8]) {
    // One codec for every log kind: they differ in which namespace they
    // carry, never in how a record is written.
    if let Some(ns) = log_ns_for(kind) {
        logs::decode(ns, record).unwrap();

        return;
    }

    // One codec for every entity kind as well, which takes the entity kind
    // the layer kind names for its id width.
    if let Some(kind) = entity_kind_for(kind) {
        entities::decode(kind, record).unwrap();

        return;
    }

    // And one codec for every state kind, for the same reason — the
    // namespace it needs is the one the kind names.
    if let Some(ns) = state_ns_for(kind) {
        state::decode(ns, record).unwrap();

        return;
    }

    match kind {
        BLOCKS => {
            blocks::decode(record).unwrap();
        }
        INDEXES => {
            indexes::decode(record).unwrap();
        }
        other => panic!("no codec for layer kind {other:?}"),
    }
}
