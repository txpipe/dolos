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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 57] = [
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
        "log-asset-mint-chunks",
        "sha256:c87071e2fbe70f006cf0100e0577a306831909d58c51426f7ecffa786e4a500b",
        2,
        109,
    ),
    (
        "log-epochs",
        "sha256:4178eef9bc0aeb206893836add3b8acb5a757a31a10fc2f46deba525a1af539a",
        2,
        61,
    ),
    (
        "log-first-mint-chunks",
        "sha256:a5fe02dd7e42e1cfdc810bfaf7e1fa4a36b91644c86561c77c26825e8b37de10",
        2,
        76,
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
        "state-asset-mints",
        "sha256:1e1b2253f88b949b5235d83bde251b60c903c0ceef073afda63e122ce2e675d7",
        2,
        110,
    ),
    (
        "state-asset-mints",
        "sha256:ab7b47381a5dae531aee3b93c82a67b3b84b5041a4f4f2f67a631fdb7d7ec848",
        2,
        110,
    ),
    (
        "state-assets",
        "sha256:970f7019432186ea22705c2728e73b08f6ae5b9fd13208a33fe5895f5faae119",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:5cd7970ca76da57d863445ced32a941b7c9083b9e24d3cf196e823b70fc8d0cd",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:40468abad6537a0a3016fd1fbfe24cd62e3769864ddf1b2d3a150437e48a7ab5",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:55b63926c1d4befc599d9c8a19493e89f82e8f515c8fd7f2638a1647155fef5c",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:abb490f9cae46a0cbf58d824a3f98f74b73f5177da8fe184cf6273c46ab2fe2b",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:874a5240997e8cc2534c53b74b40a1cf0bcc2b6180eaa69b6c106a6bf1f93027",
        2,
        85,
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
        "state-dreps",
        "sha256:638303a0e1879414890011fa271714cdb36f762939fc05b29a63fc2d52206d19",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:f903ee498315028442c33c91c81fe8e0ea1dfea2a832ee5b28814d419995518f",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:7fbc46f8b0eb29840454d5b89f8336ec17dd0d39224f5a97349be755bea27160",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:80ae9607ce4f40471574330a4c136cb6c26aa93ad82d7d7805a53ff627ef0b8d",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:4a5f0e31493a9ae338dac312de8cdbd8e06419cf182318af87edb03a28e9904f",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:16232770fe536d6c0e2eb769532e33a30f50175da8e116984f84df22eb0d1d46",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:b455a6a4e9ce01f76933f878140c9789bb6a469f0aa8c448e5138355a7048d5c",
        2,
        126,
    ),
    (
        "state-first-mints",
        "sha256:4d7d774a277734df2ca46dc1b2743bf41528b661e25a84619f1ea79d43d9a727",
        2,
        126,
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
        "state-utxos",
        "sha256:02a27db8ad3d8ce42ba4608bd6a749b08bf11affb6a7ca6edd356ebfb2cc4155",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:2caa5472d471d5fd7290a6ef5ed4d9a2494a9370caace621f3e417f42578c2ef",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:646b283b79c8da802333596178ba06619977ecae70f47038e388cd1e2d408d5b",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:1c8dcddafc12310582faa97eebda1e384a46ce8eb12bf1193e9a07a7d23548d8",
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
    "sha256:e513a81c9d4c57cb064f06dcd9923e3b7be83309d27dd0b3c42bd044d320b0cd";

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
    r#"{"diffId":"sha256:c87071e2fbe70f006cf0100e0577a306831909d58c51426f7ecffa786e4a500b","kind":"log-asset-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-asset-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":109},"#,
    r#"{"diffId":"sha256:4178eef9bc0aeb206893836add3b8acb5a757a31a10fc2f46deba525a1af539a","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:a5fe02dd7e42e1cfdc810bfaf7e1fa4a36b91644c86561c77c26825e8b37de10","kind":"log-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":76},"#,
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
    r#"{"diffId":"sha256:1e1b2253f88b949b5235d83bde251b60c903c0ceef073afda63e122ce2e675d7","kind":"state-asset-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-asset-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":110},"#,
    r#"{"diffId":"sha256:ab7b47381a5dae531aee3b93c82a67b3b84b5041a4f4f2f67a631fdb7d7ec848","kind":"state-asset-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-asset-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":110},"#,
    r#"{"diffId":"sha256:970f7019432186ea22705c2728e73b08f6ae5b9fd13208a33fe5895f5faae119","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:5cd7970ca76da57d863445ced32a941b7c9083b9e24d3cf196e823b70fc8d0cd","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:40468abad6537a0a3016fd1fbfe24cd62e3769864ddf1b2d3a150437e48a7ab5","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:55b63926c1d4befc599d9c8a19493e89f82e8f515c8fd7f2638a1647155fef5c","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:abb490f9cae46a0cbf58d824a3f98f74b73f5177da8fe184cf6273c46ab2fe2b","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:874a5240997e8cc2534c53b74b40a1cf0bcc2b6180eaa69b6c106a6bf1f93027","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:8f9643ce925ad592e62c2f983a08c428f28fb29bb51933366bc6bba836a02a83","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:787590170393d2b876f4f3fba2cb0640462ce3dd94cba7825c90534e95e77e55","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:638303a0e1879414890011fa271714cdb36f762939fc05b29a63fc2d52206d19","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:f903ee498315028442c33c91c81fe8e0ea1dfea2a832ee5b28814d419995518f","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:7fbc46f8b0eb29840454d5b89f8336ec17dd0d39224f5a97349be755bea27160","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:80ae9607ce4f40471574330a4c136cb6c26aa93ad82d7d7805a53ff627ef0b8d","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:4a5f0e31493a9ae338dac312de8cdbd8e06419cf182318af87edb03a28e9904f","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:16232770fe536d6c0e2eb769532e33a30f50175da8e116984f84df22eb0d1d46","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:b455a6a4e9ce01f76933f878140c9789bb6a469f0aa8c448e5138355a7048d5c","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:4d7d774a277734df2ca46dc1b2743bf41528b661e25a84619f1ea79d43d9a727","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":126},"#,
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
    r#"{"diffId":"sha256:02a27db8ad3d8ce42ba4608bd6a749b08bf11affb6a7ca6edd356ebfb2cc4155","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:2caa5472d471d5fd7290a6ef5ed4d9a2494a9370caace621f3e417f42578c2ef","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:646b283b79c8da802333596178ba06619977ecae70f47038e388cd1e2d408d5b","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:1c8dcddafc12310582faa97eebda1e384a46ce8eb12bf1193e9a07a7d23548d8","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"assets":1,"datums":1,"dreps":2,"epochs":3,"eras":2,"first-mint-chunks":2,"first-mints":2,"gov":4,"leader-rewards":0,"member-rewards":0,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":2,"proposals":4,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-stats":1,"accounts":16,"address-stats":1,"asset-mints":1,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"gov":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"utxos":16},"#,
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
