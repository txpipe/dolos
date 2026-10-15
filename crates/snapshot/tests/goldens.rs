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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 66] = [
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
        "log-account-epochs",
        "sha256:3df20da5471b0991ffdaf69fa45f759fb8341c2d4e2d751fd5cba6ee8827c6b8",
        2,
        102,
    ),
    (
        "log-account-stats",
        "sha256:d6014c07f910e41a978b494aabc982461446d354f6ea69e3ef9b0a4ce5dd02dd",
        2,
        101,
    ),
    (
        "log-address-stats",
        "sha256:4f8f663f48d7c5d2108d1d769ea77f2e4a050af5e45472cf061510f36736c4a8",
        2,
        130,
    ),
    (
        "log-epochs",
        "sha256:8155dd8e69e45910de973dfa5e12cc472dad8f2d5340343b9c72accd998fd2eb",
        2,
        61,
    ),
    (
        "log-first-mints",
        "sha256:d9916d14cc7b373d35bf334f7dbe729ea7ede521e7733e70ae8aba63b07235a4",
        2,
        70,
    ),
    (
        "log-payment-stats",
        "sha256:228bfc6adbc2b9e5c615a23566955679d3286c0d98e47fd7083b47eff5b2aa40",
        2,
        97,
    ),
    (
        "log-policy-first-mints",
        "sha256:df659a62bc057cdc0b90f5ab2223653526f506d60afe76afb5c1c4acc5ffdf83",
        2,
        106,
    ),
    (
        "log-pool-epochs",
        "sha256:83136992afc43be03c1aae3b2c1d3d3a848bad99faa9a4ad1cb392b9c352b228",
        2,
        95,
    ),
    (
        "log-stake-addresses",
        "sha256:5531e8ae8aef4b978ab3ba8d9762618ea692ade09483cf2a40e52f4f52112121",
        2,
        103,
    ),
    (
        "state-account-epochs",
        "sha256:90fab9f2698941985ddd5698d1d3972d0e606fd25cc3f1e51dc971fd16e2b491",
        2,
        93,
    ),
    (
        "state-account-epochs",
        "sha256:10e8e314782e69e65bd28175b0a3c1bedc70e6d30a5ee3d9e124046eb0da199f",
        2,
        93,
    ),
    (
        "state-account-stats",
        "sha256:8f31978d5bbdc835ad0fb3a01cfdf854f7a769d76d2965cefba32291579b99d3",
        2,
        100,
    ),
    (
        "state-account-stats",
        "sha256:55c63afcac11b266319bff96810045f5dba7fc7efb6b970961721579b122bbad",
        2,
        100,
    ),
    (
        "state-accounts",
        "sha256:61c12952241db1dc48fde5bcf8d7ccd56cfdb1b7579dc151d06d3929314d0fd5",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:91d697028d46280181c6c4d367a99cb6cd31dee1c2ab173fb27ad94a3f9c25e7",
        2,
        87,
    ),
    (
        "state-address-stats",
        "sha256:b601d773a99df3c3d358702a7e48774044b2de5ea73bbddea5a1bc143a6023fc",
        2,
        129,
    ),
    (
        "state-address-stats",
        "sha256:ce213adb9bac7eeab3d11f0ec0f00d5255a7f3c1c8c854b9faf2032ff36b4347",
        2,
        129,
    ),
    (
        "state-assets",
        "sha256:6576a9ad942f385cd8e4a50aa02e99f029edbbd879a874561bf9fb510a026d4f",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:e0a8ff0892d2bc76fd948f9a03b8037fdc0085337fb89dbcc004adf6b933c218",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:d341a23c7c60e8a94fdfe64872c928942ea31c917ab83bc04bd1af0da3984ca2",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:961aa62c43a7e5067d066c46ef3979b8c97e573b99eb75699786d78d37159b67",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:c0d660c1a9f29f5932528085c5c3ee2db3ed553964472b623c537b6698d10a01",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:fa543f8a639d6d74965cacc89560578df11591b9381156f62c300f3c779d93f5",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:ff962ba6ce84512d6073963f31c810e5df1eae463a1601893821187a25df931a",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:8a10b920b144c806d111a003de91d550117e97a181a9d66386bddc89a62ec0e3",
        2,
        85,
    ),
    (
        "state-dreps",
        "sha256:601caae9f595beee7c93d6f2cf470a1c8b9dee20796bff87295402db6aac473c",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:35f23ab1c88ae18f7a65f571169e521c36963abe82d5911b80d7d06d03789d88",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:5884630d5b52f801ebff853d013edb4c238c89d601915b1d749dbd73f983b489",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:b2082619f57749511ac11e9261575299b099da954c4f6da56b99cc5c9d2b9444",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:7da2890beb74b6e89ce44d54fba7a88670232eebf384add6c33ed9a16febdba7",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:a15b003a56d56ddbedbd6db9c208efb228f8772496797aeb26e3f35ccb64cfbf",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:16d6a7726520364db210a339cd22f8e4ae6fc7208c8eb74b9704d5c4a472c6e2",
        2,
        61,
    ),
    (
        "state-first-mints",
        "sha256:406a7be92f51bbb22e32ae8f783f29f0c8ff5430f03f9854a524737db806ab3b",
        2,
        61,
    ),
    (
        "state-gov",
        "sha256:cd5a3b00e99070a89c6e70f15c5d054f7fcd706967238d1818a8dfd65ec6da42",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:d38a1f1b986c1c0436535f9c46555960c652115b2d96d7407655b7d6077bd6f0",
        2,
        49,
    ),
    (
        "state-payment-stats",
        "sha256:c8237a98ead16917d641334a5f1406d53518acee47bb198210d6b3b97be9a74f",
        2,
        88,
    ),
    (
        "state-payment-stats",
        "sha256:546c36c75d468f69c24c5d6405a1d320d89e481e25925050bdd61d352059b112",
        2,
        88,
    ),
    (
        "state-pending-mirs",
        "sha256:b8037de2e7debe88733b72ee639a2fcb9205fac0bf23c290312987618505155f",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:f66fd3c9252e618b56910bcc90ef00663ea5997210fe696ef2110910b764b991",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:8aabb9f2137df2170b48b1962862afc01462e25daf713308d1561836eabf3f57",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:9a2529a6d0d45e014e5fe6c26f4002166e95e73e51f53b82f397c2ed6d9ba2cb",
        2,
        94,
    ),
    (
        "state-policy-first-mints",
        "sha256:74edd6de5e426f633089fed4495a6a07740f74551916475aae99a682ddf2155f",
        2,
        98,
    ),
    (
        "state-policy-first-mints",
        "sha256:ab2a2dba28c6b22a336236fd30687f2d705e3300548bba5b11bdbefb0193f4d2",
        2,
        98,
    ),
    (
        "state-pool-epochs",
        "sha256:9344bf8c5c369008ade403b6a37ee9995d1d779b7cc99185451f60585bcabab4",
        2,
        86,
    ),
    (
        "state-pool-epochs",
        "sha256:c155303f8708ef9c21bdf575173179c0c2f9d4d9a6c300b8a226dec0f3e19e05",
        2,
        86,
    ),
    (
        "state-pools",
        "sha256:46af3c7cc7513800d1f004d299582071b7f37a0f82fe6328a87cb3c91f6f5bb1",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:816e7af6f127e36c31ff82e45d7ec1d3656a90329441b23343684254b054b7b2",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:82ef87ead1279cada167de6f13a6e12520df2a321c503a523092f3233d229bba",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:7b5233d3e00f7036148cdfa508ef46ba6dd04ca583ef37ee8a01f83882f166ba",
        2,
        92,
    ),
    (
        "state-staged-first-mints",
        "sha256:20d4a2058c4cbb3e47585c717b7ef6a2223bbf6c91053c5bb9d758680f101d01",
        2,
        134,
    ),
    (
        "state-staged-first-mints",
        "sha256:4ae6261c4614db3de9aabde28812a62537547abebb47f010e78f6bf3aca1382e",
        2,
        134,
    ),
    (
        "state-staged-payment-addresses",
        "sha256:8a039a5c872961a5bd33638ef231f8fbc91ddb85f658e1fa0715036e062f4601",
        2,
        137,
    ),
    (
        "state-staged-payment-addresses",
        "sha256:9a921ae5585f3be7cc1c8b5a5d677308935ec47c1d52b61452109e34fe92ac18",
        2,
        137,
    ),
    (
        "state-staged-payment-stats",
        "sha256:d207c3afd0921aea44f5aa181144cb65d46ed22dcafd80190a6431583064ee5d",
        2,
        104,
    ),
    (
        "state-staged-payment-stats",
        "sha256:5e30bbd228b9ef325f4338b1ab16827a62bb96d3a392673f2402fcb3af0e3dd2",
        2,
        104,
    ),
    (
        "state-staged-stake-addresses",
        "sha256:6b761ce516c0605d4d1fa43d17e426346679d3390f2e4d3506741871f7ff6b84",
        2,
        171,
    ),
    (
        "state-staged-stake-addresses",
        "sha256:f388f56521dce1a35cd41fb2ac7842f3738e7674717f4a72bbb71ce82a2c5956",
        2,
        171,
    ),
    (
        "state-stake-addresses",
        "sha256:425581a02e7cb4777791433adb9b0c2f64ae4ddc85276f69301b56b2ed221a30",
        2,
        94,
    ),
    (
        "state-stake-addresses",
        "sha256:b3930e8b76d3816a59d1273193108db760a69de1c40122b4833fb3731d2d2453",
        2,
        94,
    ),
    (
        "state-utxos",
        "sha256:3819aa0751483be44684c149ce4406c3308037f037086e6e005c7171f8d5332a",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:f9593245302be14d103478d1c993ec2b1f6e528141779c3c20612439d8a2837b",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:dc708ef8509c6f92b4948d1e8821c5cc5c33a1323bad404c68153ec64e43e0a1",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:a13607aa4d8043856f5518667ff82b081dbcc10677cd4eb8a37bc33348f9dc6f",
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
    "sha256:580308b96b135d0282b290998130e7fc04274d66b276d6413c126fffe5cdaf76";

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
    r#"{"diffId":"sha256:3df20da5471b0991ffdaf69fa45f759fb8341c2d4e2d751fd5cba6ee8827c6b8","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:d6014c07f910e41a978b494aabc982461446d354f6ea69e3ef9b0a4ce5dd02dd","kind":"log-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":101},"#,
    r#"{"diffId":"sha256:4f8f663f48d7c5d2108d1d769ea77f2e4a050af5e45472cf061510f36736c4a8","kind":"log-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":130},"#,
    r#"{"diffId":"sha256:8155dd8e69e45910de973dfa5e12cc472dad8f2d5340343b9c72accd998fd2eb","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:d9916d14cc7b373d35bf334f7dbe729ea7ede521e7733e70ae8aba63b07235a4","kind":"log-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":70},"#,
    r#"{"diffId":"sha256:228bfc6adbc2b9e5c615a23566955679d3286c0d98e47fd7083b47eff5b2aa40","kind":"log-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":97},"#,
    r#"{"diffId":"sha256:df659a62bc057cdc0b90f5ab2223653526f506d60afe76afb5c1c4acc5ffdf83","kind":"log-policy-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:83136992afc43be03c1aae3b2c1d3d3a848bad99faa9a4ad1cb392b9c352b228","kind":"log-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":95},"#,
    r#"{"diffId":"sha256:5531e8ae8aef4b978ab3ba8d9762618ea692ade09483cf2a40e52f4f52112121","kind":"log-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.log-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":103},"#,
    r#"{"diffId":"sha256:90fab9f2698941985ddd5698d1d3972d0e606fd25cc3f1e51dc971fd16e2b491","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:10e8e314782e69e65bd28175b0a3c1bedc70e6d30a5ee3d9e124046eb0da199f","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:8f31978d5bbdc835ad0fb3a01cfdf854f7a769d76d2965cefba32291579b99d3","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:55c63afcac11b266319bff96810045f5dba7fc7efb6b970961721579b122bbad","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:61c12952241db1dc48fde5bcf8d7ccd56cfdb1b7579dc151d06d3929314d0fd5","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:91d697028d46280181c6c4d367a99cb6cd31dee1c2ab173fb27ad94a3f9c25e7","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:b601d773a99df3c3d358702a7e48774044b2de5ea73bbddea5a1bc143a6023fc","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:ce213adb9bac7eeab3d11f0ec0f00d5255a7f3c1c8c854b9faf2032ff36b4347","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:6576a9ad942f385cd8e4a50aa02e99f029edbbd879a874561bf9fb510a026d4f","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:e0a8ff0892d2bc76fd948f9a03b8037fdc0085337fb89dbcc004adf6b933c218","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:d341a23c7c60e8a94fdfe64872c928942ea31c917ab83bc04bd1af0da3984ca2","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:961aa62c43a7e5067d066c46ef3979b8c97e573b99eb75699786d78d37159b67","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c0d660c1a9f29f5932528085c5c3ee2db3ed553964472b623c537b6698d10a01","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:fa543f8a639d6d74965cacc89560578df11591b9381156f62c300f3c779d93f5","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:ff962ba6ce84512d6073963f31c810e5df1eae463a1601893821187a25df931a","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:8a10b920b144c806d111a003de91d550117e97a181a9d66386bddc89a62ec0e3","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:601caae9f595beee7c93d6f2cf470a1c8b9dee20796bff87295402db6aac473c","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:35f23ab1c88ae18f7a65f571169e521c36963abe82d5911b80d7d06d03789d88","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:5884630d5b52f801ebff853d013edb4c238c89d601915b1d749dbd73f983b489","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:b2082619f57749511ac11e9261575299b099da954c4f6da56b99cc5c9d2b9444","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:7da2890beb74b6e89ce44d54fba7a88670232eebf384add6c33ed9a16febdba7","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:a15b003a56d56ddbedbd6db9c208efb228f8772496797aeb26e3f35ccb64cfbf","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:16d6a7726520364db210a339cd22f8e4ae6fc7208c8eb74b9704d5c4a472c6e2","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:406a7be92f51bbb22e32ae8f783f29f0c8ff5430f03f9854a524737db806ab3b","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:cd5a3b00e99070a89c6e70f15c5d054f7fcd706967238d1818a8dfd65ec6da42","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:d38a1f1b986c1c0436535f9c46555960c652115b2d96d7407655b7d6077bd6f0","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:c8237a98ead16917d641334a5f1406d53518acee47bb198210d6b3b97be9a74f","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":88},"#,
    r#"{"diffId":"sha256:546c36c75d468f69c24c5d6405a1d320d89e481e25925050bdd61d352059b112","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":88},"#,
    r#"{"diffId":"sha256:b8037de2e7debe88733b72ee639a2fcb9205fac0bf23c290312987618505155f","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:f66fd3c9252e618b56910bcc90ef00663ea5997210fe696ef2110910b764b991","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:8aabb9f2137df2170b48b1962862afc01462e25daf713308d1561836eabf3f57","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:9a2529a6d0d45e014e5fe6c26f4002166e95e73e51f53b82f397c2ed6d9ba2cb","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:74edd6de5e426f633089fed4495a6a07740f74551916475aae99a682ddf2155f","kind":"state-policy-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-policy-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":98},"#,
    r#"{"diffId":"sha256:ab2a2dba28c6b22a336236fd30687f2d705e3300548bba5b11bdbefb0193f4d2","kind":"state-policy-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-policy-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":98},"#,
    r#"{"diffId":"sha256:9344bf8c5c369008ade403b6a37ee9995d1d779b7cc99185451f60585bcabab4","kind":"state-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":86},"#,
    r#"{"diffId":"sha256:c155303f8708ef9c21bdf575173179c0c2f9d4d9a6c300b8a226dec0f3e19e05","kind":"state-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":86},"#,
    r#"{"diffId":"sha256:46af3c7cc7513800d1f004d299582071b7f37a0f82fe6328a87cb3c91f6f5bb1","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:816e7af6f127e36c31ff82e45d7ec1d3656a90329441b23343684254b054b7b2","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:82ef87ead1279cada167de6f13a6e12520df2a321c503a523092f3233d229bba","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:7b5233d3e00f7036148cdfa508ef46ba6dd04ca583ef37ee8a01f83882f166ba","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:20d4a2058c4cbb3e47585c717b7ef6a2223bbf6c91053c5bb9d758680f101d01","kind":"state-staged-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":134},"#,
    r#"{"diffId":"sha256:4ae6261c4614db3de9aabde28812a62537547abebb47f010e78f6bf3aca1382e","kind":"state-staged-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":134},"#,
    r#"{"diffId":"sha256:8a039a5c872961a5bd33638ef231f8fbc91ddb85f658e1fa0715036e062f4601","kind":"state-staged-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":137},"#,
    r#"{"diffId":"sha256:9a921ae5585f3be7cc1c8b5a5d677308935ec47c1d52b61452109e34fe92ac18","kind":"state-staged-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":137},"#,
    r#"{"diffId":"sha256:d207c3afd0921aea44f5aa181144cb65d46ed22dcafd80190a6431583064ee5d","kind":"state-staged-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":104},"#,
    r#"{"diffId":"sha256:5e30bbd228b9ef325f4338b1ab16827a62bb96d3a392673f2402fcb3af0e3dd2","kind":"state-staged-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":104},"#,
    r#"{"diffId":"sha256:6b761ce516c0605d4d1fa43d17e426346679d3390f2e4d3506741871f7ff6b84","kind":"state-staged-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":171},"#,
    r#"{"diffId":"sha256:f388f56521dce1a35cd41fb2ac7842f3738e7674717f4a72bbb71ce82a2c5956","kind":"state-staged-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":171},"#,
    r#"{"diffId":"sha256:425581a02e7cb4777791433adb9b0c2f64ae4ddc85276f69301b56b2ed221a30","kind":"state-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:b3930e8b76d3816a59d1273193108db760a69de1c40122b4833fb3731d2d2453","kind":"state-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:3819aa0751483be44684c149ce4406c3308037f037086e6e005c7171f8d5332a","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:f9593245302be14d103478d1c993ec2b1f6e528141779c3c20612439d8a2837b","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:dc708ef8509c6f92b4948d1e8821c5cc5c33a1323bad404c68153ec64e43e0a1","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:a13607aa4d8043856f5518667ff82b081dbcc10677cd4eb8a37bc33348f9dc6f","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"assets":1,"datums":1,"dreps":2,"epochs":3,"eras":2,"first-mints":2,"gov":4,"leader-rewards":0,"member-rewards":0,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-first-mints":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":2,"proposals":4,"staged-first-mints":2,"staged-payment-addresses":1,"staged-payment-stats":1,"staged-stake-addresses":1,"stake-addresses":1,"utxos":1},"#,
    r#""shards":{"account-epochs":1,"account-stats":1,"accounts":16,"address-stats":1,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"gov":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-first-mints":1,"pool-epochs":1,"pools":1,"proposals":1,"staged-first-mints":1,"staged-payment-addresses":1,"staged-payment-stats":1,"staged-stake-addresses":1,"stake-addresses":1,"utxos":16},"#,
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
