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
//! dimension name and exact-record kind literal, every `log-{ns}` and
//! `state-{ns}` kind string — which is where the namespace strings live now
//! that neither record names one — the layer header and scope shapes, and the
//! `position`/`parameters` key spellings, the shard and schema maps included.

mod common;

use common::*;
use dolos_core::{BlockHash, ChainPoint};
use dolos_snapshot::{
    layers::{blocks, digests, indexes, logs, state},
    log_ns_for, state_ns_for, DolosProfile, RetainedEpochs, BLOCKS, DIGESTS, INDEXES,
};
use stelae::{
    dir::SteleDir,
    frame::Limits,
    inscription::{HistoryEntry, Inscription},
    Digest, SteleReader, SteleWriter,
};

/// `(kind, diffId, records, uncompressedSize)`, in inscription order.
///
/// `records` counts the protocol's header record, as a descriptor does.
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 61] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:a0179ae003b2f11abfb41885369b6cd1a6c8996630fa49aa3bed238a74d1b433",
        23,
        619,
    ),
    (
        "log-account-address-chunks",
        "sha256:620b0484b8c0d6e305c6c9746950c9f9dd186b256fb2febf030d0e75507b3fe3",
        2,
        111,
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
        "log-epochs",
        "sha256:4d2741b35a2fab1aa2d7ae27a46f6b8dc3792a5b2994bd519147c997c152b759",
        2,
        61,
    ),
    (
        "log-first-mint-chunks",
        "sha256:f0df9e4ec72d62a077a9b945dde15af39da05a274afe9b8c6d551ed3bee53c70",
        2,
        76,
    ),
    (
        "log-first-script-chunks",
        "sha256:23956ae8f9c43f0cb91b25aa5157d6636eb0c77e7601ad14378cd81569c82820",
        2,
        78,
    ),
    (
        "log-payment-address-chunks",
        "sha256:60462c3766bf76e6a496fcba11a2c199668a7d59496136193f3d30872c588bdf",
        2,
        107,
    ),
    (
        "log-policy-mint-chunks",
        "sha256:41be90cf1f534ceb89e34151b1ab1d8d3cdff1e183929eabce6f47b0295cfbd1",
        2,
        106,
    ),
    (
        "log-pool-epochs",
        "sha256:143e23942c0e849e1c620ee75d8de1affc5a789d7259a9af876fd702abebf76e",
        2,
        95,
    ),
    (
        "state-account-addresses",
        "sha256:9c4a51d9ef3969cea7075cf31eaadf82efc9bef6e5427296976201af4663d8e5",
        2,
        165,
    ),
    (
        "state-account-addresses",
        "sha256:5296f2db380326b740501e5706a980bf5dc8f96d8fc6b5c3e37f825ebc43b204",
        2,
        165,
    ),
    (
        "state-account-stats",
        "sha256:f83b581901dec97c2fb1d0a696a6c4d0606268229843f9edc0b60eb955f084aa",
        2,
        100,
    ),
    (
        "state-account-stats",
        "sha256:9b8ac7a591e85ccd8db28fed9a68a56f382b148eb103a3e368c3275a6ae50d18",
        2,
        100,
    ),
    (
        "state-accounts",
        "sha256:b83b89a357ff5ab3f738c2b3ef238fc757d11ec59103caa7518d008de458d7b3",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:f0d22f5dba398a9dc1a019aadb87397abfe163d0c4c204c540379681cd25ed12",
        2,
        87,
    ),
    (
        "state-address-stats",
        "sha256:7e213415880a0204a489ba415e224c44f119e64c630417dbb18fbf1ba9188f5a",
        2,
        129,
    ),
    (
        "state-address-stats",
        "sha256:24f9fd95534a65b4545874d62635d49f1bcc8cafdc8d1ef4e096938a3759fc57",
        2,
        129,
    ),
    (
        "state-assets",
        "sha256:b3245ac0b24b08885c3b78227a3d2d7792a20c061ad22931e9de15cf576d7a98",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:c04849646e7500199fd2125a678e0792fb4afc38d14afb12222804ae50ef6ad1",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:0f51ce1103a235397b071363f2874b0ee8d719beb903a1d8deedace236c1304d",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:8409ca3904487f879e04b59ad925cc396ae5f33a0cea0f6fca055049e8d0e88a",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:c0636a49414a0d2364a34d1da00eafcf09c8c24e30fb0dc2e2ad7a617b5d2e26",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:17b8d6ecef148a188789a33a23cf9aaac8ef087ebb05134bbd713c5a6c830b96",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:f7e000ca94cd68d6d4e9a4c9d760f2285723e51f5efb226a9176516a2e6fd8ea",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:bf69b05259405f578d98a23b1265d840b681c42d240afc9338cb8733ff4a3a3d",
        2,
        85,
    ),
    (
        "state-dreps",
        "sha256:9535350d2004dc1059a3a88245d868009b63d16c833ae7593cc7dfc76144cc62",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:ace3f999dc64d5282196ee3e5fcde2cd92fedf04fd0fafcbe9a000ac5232512b",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:79e8d6a7f2f20e63e924fe4c8bf62de19033847e740dc223672feb0f5e15e815",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:614498a2c58d6bbad97266bc438e0a5da9263f7310e355a4be75df5c5d7fa29e",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:eece91caf082c8b5bffa73d0d96ba95834337e0c43ab9aa9224f085a58d38d4b",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:3eeab9c4ce7987356e8a1c135194f4be84629830dfec753aa40cacd4d76b6780",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:ef2ea19e743ec97838e664561168fecdf6a83c23ca9656b26634ab2fd2404924",
        2,
        126,
    ),
    (
        "state-first-mints",
        "sha256:cdc622ac84bab03f5e5082dbbaac8d8a3bc9343df9e901c394757c76c24c5bcf",
        2,
        126,
    ),
    (
        "state-first-scripts",
        "sha256:24f9acda9741566ffd1d34e3cc89ec3e520cede995c1fbd9ad21050b2023b078",
        2,
        96,
    ),
    (
        "state-first-scripts",
        "sha256:05c2211591d4bea03dd1cffae97ec5c7497c6a7b5e1bc9050cb90a945b156c67",
        2,
        96,
    ),
    (
        "state-gov",
        "sha256:b2747b54c59884a87aee103a3e729020f39fb2b818ec21d0a97528756658cdb4",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:d59a9f59c5674c7f451c9721106e881c2420895269ad57c1f72f5f867dea710c",
        2,
        49,
    ),
    (
        "state-payment-addresses",
        "sha256:a98fb08e1416c104dc540421366aff173929f505bebcc1e83ba7c605ec9446a8",
        2,
        129,
    ),
    (
        "state-payment-addresses",
        "sha256:0b7aa8c380cfff5f1e2328f14fccabfe1c32e3cc1c498cd2e81c0c6c84632e15",
        2,
        129,
    ),
    (
        "state-payment-stats",
        "sha256:f33e62b8fb41e9a6a6a3a9f82c6fc06290dc66aa90281296a9801f360e82ef4f",
        2,
        96,
    ),
    (
        "state-payment-stats",
        "sha256:bce9481ed0458a1ef1ad5560724c132d8fe47ce0d054a83c0a6c09eec1964617",
        2,
        96,
    ),
    (
        "state-pending-mirs",
        "sha256:9ee073944f7b25b78fe0a4853882e46f1f2616b7ae74018bae9513f199ea469d",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:d015dd42ea4c10ab66f70b52322dbb8bebdff70a9f6d2978d6d26d24bafd207f",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:9f3bfc7e165a7d2acce27bb15267f5bfe9bcd64d511cf1e72e9fa541fe5006c9",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:2d00158f7075343bbbd3723c3f5569d5cfa05824ee006b9ef407a7d9a21084ad",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:a03dd6be8677f3198b95054ba81ce450d223933c0d8ad9f9ba472ba3b9b2dbe1",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:04a1b78dabb19682c565cc21f15f4c41f99409cdc7a24bf8ff7e9b65d9fd676a",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:e3c85ac9eeff99531b3b3899ab84cac9b5a1965cebe1aa86127d1c40baae32fa",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:a75d7594a2906c02b71191477d5fdf25e5846ace2be92ae04645051bb6c08904",
        2,
        92,
    ),
    (
        "state-scripts",
        "sha256:f0f927cb96e56180f369e50fda6264ef8141626f1db80ac7560f4ec3a9c12043",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:608fc0b4c66356771b50fdb9f438525dd811d5d2c24a93fb80a778d87935ecaa",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:7883b701523a03a8479da4330220d1a58d32369a77fea5dcb5d985a87ac7ad53",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:28a63578809a0672c8998a1102dbdf9f2a4d1829c88a6aa0b23ef3f88f483aa7",
        2,
        82,
    ),
    (
        "state-utxos",
        "sha256:41a8de1e27ae768e75e6a36daf34df91a3f736d3018c9407535166befa5422a3",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:ea9f110ca3f7f6bf783c847a7b83f5a5625b0be30fb33cb2b57ac34baa904434",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:bb364ec7e475c4dbd52e69f14d2b23f2af4b308a4fcfa65481c7adabf9b59259",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:f7e26ab676685fe37b02dbf02bb2de937dd86d15c6158841d41826fa82e13634",
        2,
        91,
    ),
    (
        DIGESTS,
        "sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80",
        3,
        250,
    ),
];

/// The stele's identity: sha256 of the canonical inscription.
const GOLDEN_INSCRIPTION: &str =
    "sha256:daed5f3c0f683d053f130bfbd9606caf3b1b0ce33ec407c4bc29897b9b2990c3";

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

/// Write the whole fixture stele into `root`: its layers and an inscription.
fn write_stele(root: &std::path::Path) -> (Inscription, Digest) {
    let stele = SteleDir::create(root).unwrap();

    let point = ChainPoint::Specific(END_SLOT, BlockHash::new(POINT_HASH));

    let mut inscription = Inscription::new(
        &DolosProfile,
        EPOCH,
        dolos_snapshot::position(&network(), &point, EPOCH).unwrap(),
        dolos_snapshot::parameters(&RetainedEpochs::new(vec![DUMP_EPOCH]).unwrap()),
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
    let stele = SteleDir::open(first.path()).unwrap();
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

    let second_index = SteleDir::open(second.path()).unwrap().blob_index().unwrap();
    for descriptor in &inscription.layers {
        assert_eq!(
            index.blob_for(&descriptor.diff_id),
            second_index.blob_for(&descriptor.diff_id),
            "layer {:?}",
            descriptor.kind,
        );
    }
}

/// The descriptors a `SteleDir` writes are the numbers the per-kind goldens
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
    assert!(canonical.contains("application/vnd.dolos.stele.blocks.v1+zstd"));
    assert!(!canonical.contains("vnd.stelae.stele"));
}

/// The whole document, so the diff of a format change is readable.
///
/// JCS sorts object keys, so the layout below is the protocol's, not this
/// crate's — but every *string* in it is this profile's, and that is what the
/// literal is for.
const CANONICAL_INSCRIPTION: &str = concat!(
    r#"{"compression":{"algo":"zstd","level":9},"history":[{"inscriptionDigest":"sha256:5555555555555555555555555555555555555555555555555555555555555555","sequence":5},{"inscriptionDigest":"sha256:6666666666666666666666666666666666666666666666666666666666666666","sequence":6}],"layers":["#,
    r#"{"diffId":"sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106","kind":"blocks","#,
    r#""mediaType":"application/vnd.dolos.stele.blocks.v1+zstd","records":4,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":167},"#,
    r#"{"diffId":"sha256:a0179ae003b2f11abfb41885369b6cd1a6c8996630fa49aa3bed238a74d1b433","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":23,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":619},"#,
    r#"{"diffId":"sha256:620b0484b8c0d6e305c6c9746950c9f9dd186b256fb2febf030d0e75507b3fe3","kind":"log-account-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":111},"#,
    r#"{"diffId":"sha256:9cfebfa5dd85cadd44e59d0e51b26c3adeca347f0aea30188d96dbf1eeb22be7","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:e055e50725da443014dab37602e16a8b40bbefc64183ac7a9e77159ec952143f","kind":"log-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":101},"#,
    r#"{"diffId":"sha256:de7b9f85d5fc130fb4ed732da9448572233907f4966e5ac435530669239c87aa","kind":"log-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":130},"#,
    r#"{"diffId":"sha256:4d2741b35a2fab1aa2d7ae27a46f6b8dc3792a5b2994bd519147c997c152b759","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:f0df9e4ec72d62a077a9b945dde15af39da05a274afe9b8c6d551ed3bee53c70","kind":"log-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":76},"#,
    r#"{"diffId":"sha256:23956ae8f9c43f0cb91b25aa5157d6636eb0c77e7601ad14378cd81569c82820","kind":"log-first-script-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-script-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":78},"#,
    r#"{"diffId":"sha256:60462c3766bf76e6a496fcba11a2c199668a7d59496136193f3d30872c588bdf","kind":"log-payment-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-payment-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":107},"#,
    r#"{"diffId":"sha256:41be90cf1f534ceb89e34151b1ab1d8d3cdff1e183929eabce6f47b0295cfbd1","kind":"log-policy-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:143e23942c0e849e1c620ee75d8de1affc5a789d7259a9af876fd702abebf76e","kind":"log-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":95},"#,
    r#"{"diffId":"sha256:9c4a51d9ef3969cea7075cf31eaadf82efc9bef6e5427296976201af4663d8e5","kind":"state-account-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":165},"#,
    r#"{"diffId":"sha256:5296f2db380326b740501e5706a980bf5dc8f96d8fc6b5c3e37f825ebc43b204","kind":"state-account-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":165},"#,
    r#"{"diffId":"sha256:f83b581901dec97c2fb1d0a696a6c4d0606268229843f9edc0b60eb955f084aa","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:9b8ac7a591e85ccd8db28fed9a68a56f382b148eb103a3e368c3275a6ae50d18","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:b83b89a357ff5ab3f738c2b3ef238fc757d11ec59103caa7518d008de458d7b3","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:f0d22f5dba398a9dc1a019aadb87397abfe163d0c4c204c540379681cd25ed12","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:7e213415880a0204a489ba415e224c44f119e64c630417dbb18fbf1ba9188f5a","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:24f9fd95534a65b4545874d62635d49f1bcc8cafdc8d1ef4e096938a3759fc57","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:b3245ac0b24b08885c3b78227a3d2d7792a20c061ad22931e9de15cf576d7a98","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c04849646e7500199fd2125a678e0792fb4afc38d14afb12222804ae50ef6ad1","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:0f51ce1103a235397b071363f2874b0ee8d719beb903a1d8deedace236c1304d","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:8409ca3904487f879e04b59ad925cc396ae5f33a0cea0f6fca055049e8d0e88a","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c0636a49414a0d2364a34d1da00eafcf09c8c24e30fb0dc2e2ad7a617b5d2e26","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:17b8d6ecef148a188789a33a23cf9aaac8ef087ebb05134bbd713c5a6c830b96","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:f7e000ca94cd68d6d4e9a4c9d760f2285723e51f5efb226a9176516a2e6fd8ea","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:bf69b05259405f578d98a23b1265d840b681c42d240afc9338cb8733ff4a3a3d","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:9535350d2004dc1059a3a88245d868009b63d16c833ae7593cc7dfc76144cc62","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:ace3f999dc64d5282196ee3e5fcde2cd92fedf04fd0fafcbe9a000ac5232512b","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:79e8d6a7f2f20e63e924fe4c8bf62de19033847e740dc223672feb0f5e15e815","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:614498a2c58d6bbad97266bc438e0a5da9263f7310e355a4be75df5c5d7fa29e","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:eece91caf082c8b5bffa73d0d96ba95834337e0c43ab9aa9224f085a58d38d4b","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:3eeab9c4ce7987356e8a1c135194f4be84629830dfec753aa40cacd4d76b6780","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:ef2ea19e743ec97838e664561168fecdf6a83c23ca9656b26634ab2fd2404924","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:cdc622ac84bab03f5e5082dbbaac8d8a3bc9343df9e901c394757c76c24c5bcf","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:24f9acda9741566ffd1d34e3cc89ec3e520cede995c1fbd9ad21050b2023b078","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:05c2211591d4bea03dd1cffae97ec5c7497c6a7b5e1bc9050cb90a945b156c67","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:b2747b54c59884a87aee103a3e729020f39fb2b818ec21d0a97528756658cdb4","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:d59a9f59c5674c7f451c9721106e881c2420895269ad57c1f72f5f867dea710c","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:a98fb08e1416c104dc540421366aff173929f505bebcc1e83ba7c605ec9446a8","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:0b7aa8c380cfff5f1e2328f14fccabfe1c32e3cc1c498cd2e81c0c6c84632e15","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:f33e62b8fb41e9a6a6a3a9f82c6fc06290dc66aa90281296a9801f360e82ef4f","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:bce9481ed0458a1ef1ad5560724c132d8fe47ce0d054a83c0a6c09eec1964617","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:9ee073944f7b25b78fe0a4853882e46f1f2616b7ae74018bae9513f199ea469d","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:d015dd42ea4c10ab66f70b52322dbb8bebdff70a9f6d2978d6d26d24bafd207f","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:9f3bfc7e165a7d2acce27bb15267f5bfe9bcd64d511cf1e72e9fa541fe5006c9","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:2d00158f7075343bbbd3723c3f5569d5cfa05824ee006b9ef407a7d9a21084ad","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:a03dd6be8677f3198b95054ba81ce450d223933c0d8ad9f9ba472ba3b9b2dbe1","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:04a1b78dabb19682c565cc21f15f4c41f99409cdc7a24bf8ff7e9b65d9fd676a","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:e3c85ac9eeff99531b3b3899ab84cac9b5a1965cebe1aa86127d1c40baae32fa","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:a75d7594a2906c02b71191477d5fdf25e5846ace2be92ae04645051bb6c08904","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:f0f927cb96e56180f369e50fda6264ef8141626f1db80ac7560f4ec3a9c12043","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:608fc0b4c66356771b50fdb9f438525dd811d5d2c24a93fb80a778d87935ecaa","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:7883b701523a03a8479da4330220d1a58d32369a77fea5dcb5d985a87ac7ad53","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:28a63578809a0672c8998a1102dbdf9f2a4d1829c88a6aa0b23ef3f88f483aa7","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:41a8de1e27ae768e75e6a36daf34df91a3f736d3018c9407535166befa5422a3","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:ea9f110ca3f7f6bf783c847a7b83f5a5625b0be30fb33cb2b57ac34baa904434","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:bb364ec7e475c4dbd52e69f14d2b23f2af4b308a4fcfa65481c7adabf9b59259","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:f7e26ab676685fe37b02dbf02bb2de937dd86d15c6158841d41826fa82e13634","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"assets":1,"datums":1,"dreps":2,"epochs":3,"eras":2,"first-mint-chunks":2,"first-mints":2,"first-script-chunks":1,"first-scripts":1,"gov":4,"leader-rewards":0,"member-rewards":0,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":2,"proposals":4,"scripts":1,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-stats":1,"accounts":16,"address-stats":1,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"first-scripts":1,"gov":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"scripts":16,"utxos":16},"#,
    r#""stateEpochs":[4]"#,
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
        DIGESTS => {
            digests::decode(record).unwrap();
        }
        other => panic!("no codec for layer kind {other:?}"),
    }
}
