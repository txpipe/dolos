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
        "log-first-script-chunks",
        "sha256:cfc9e747fbb8fc79be16fe8888d4f60b11ad6d2f73cb7c04965ba295be927709",
        2,
        78,
    ),
    (
        "log-payment-address-chunks",
        "sha256:61d451abc6d7869e00057d721e3f7522953501a58ab29a7df88c6d78fedd53d7",
        2,
        107,
    ),
    (
        "log-policy-mint-chunks",
        "sha256:13e1629c40b94bc5e88b4efd5aa16628b982902b3aef7de4c08b7a01b672728d",
        2,
        106,
    ),
    (
        "log-pool-epochs",
        "sha256:2d8032bb0ac17e856ab2e760a568b7fc7ee19d6a18c20839580cc3d0daa3add1",
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
        "state-first-scripts",
        "sha256:57cec0094bc6801fa606715cef6c5c05fb7c239bb22ac6c3943303c497679c42",
        2,
        96,
    ),
    (
        "state-first-scripts",
        "sha256:c627c4c5b4502503b5ebccbce52584cbb7ee543e0ad162cc2e5d01d4c90d6777",
        2,
        96,
    ),
    (
        "state-gov",
        "sha256:732d42a28acbd3abd4bf3dfc1eb452fa332c371d8601e3c3d43797415692439f",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:c77ef149c9a351b638ec3a9d9f91ba498e19a44af6d3a72f8e6ee91385c760f4",
        2,
        49,
    ),
    (
        "state-metadata-labels",
        "sha256:9016a1c7d55c0a2523766dba66827d7b5c59ce1ba65068cc91c274f47f503b5f",
        2,
        69,
    ),
    (
        "state-metadata-labels",
        "sha256:a202395bb321da4047cbee71a851fc8a458f66a3094cebf9cb1ae8cdaf874805",
        2,
        69,
    ),
    (
        "state-payment-addresses",
        "sha256:df527420559c35e6714fac6464e604f33ee50d03fe9470bada31f71fb86ff574",
        2,
        129,
    ),
    (
        "state-payment-addresses",
        "sha256:e1b48405a880c4f338cd31b80f3ab62b489e163bc8af9bd7a7e5a823088e8444",
        2,
        129,
    ),
    (
        "state-payment-stats",
        "sha256:83ebf9d14ea3125fe8e29597050d7bb5554cc56e0ae5452f403768637f954884",
        2,
        96,
    ),
    (
        "state-payment-stats",
        "sha256:ef1df0567e7f9231763aae41feadd6608e63623a9e64c74e4f99323121cfe97f",
        2,
        96,
    ),
    (
        "state-pending-mirs",
        "sha256:82579f22d260a4cd7e45883a5e401d82726d053e1bf3e4098fea887fdf7a5d13",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:bb3dcdaca7726870ec256e018add03f04940ed1cdbc0094032bb7214dc22cab8",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:a99df1b648def4e9da03c594cd759ddbe197be1670ab8c4d4ac082555dec0b06",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:a661cf64259ef6ebe6071aa850c8a0b46ec1934db138d9d63c970fb60ae1ba90",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:f675fcca8599c8f334c9a8be5e2c2dc36e7ac6bfc9f1ab29238322d78e509a0f",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:0f4e2a3f2246f2e702d694c535e5eff6280ff4494fa1c14ec60f858cd724c94c",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:305c67536c0d380cc899ab04e6e177ed6571a776d9edbe37fe7f666a5ad3625a",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:fadd3f881ccb0868c8735094ce571df6153ec6b773cc9bfd0ea61cabf1a2240d",
        2,
        92,
    ),
    (
        "state-scripts",
        "sha256:97b7f4dc88289d39230d36c3795783e2f5d6b2100a81482517ab57c31ee94eb2",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:cba0735fdec007e3d3ca320d3d0b713abd0cffccd6874ddb9d4d5a6cdc3e9a20",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:43648ba11c81dd241a4989e1c778ef6a47901ae1640c9787e6e4325ae74ed24f",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:b5a1ca751c3a1b42944e1ca32e6fc984bf7be04359f4bf3b216c3ac0d857b4a9",
        2,
        82,
    ),
    (
        "state-utxos",
        "sha256:76e24229e77461a879b84e988831588366a413c2c8e249f8c8cacd35f32a72dc",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:8bbf2304394dc53255c4940b78b3c2eb02e470979246ccb399ec3ceec26c8df6",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:5db3baae6fc2c3f2296ae9dc35e2dfeef1fefc3dc6aa03cf4fde75d27ce9c67e",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:94e004d2d79e40e0ed338bff1243b738db98a8307274fe292c0baf8b8223f284",
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
    "sha256:1e6c47a225fe8748c55926795a87a09b71f693cc192ad3e00023959a42a9363d";

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
    r#"{"diffId":"sha256:cfc9e747fbb8fc79be16fe8888d4f60b11ad6d2f73cb7c04965ba295be927709","kind":"log-first-script-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-script-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":78},"#,
    r#"{"diffId":"sha256:61d451abc6d7869e00057d721e3f7522953501a58ab29a7df88c6d78fedd53d7","kind":"log-payment-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-payment-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":107},"#,
    r#"{"diffId":"sha256:13e1629c40b94bc5e88b4efd5aa16628b982902b3aef7de4c08b7a01b672728d","kind":"log-policy-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:2d8032bb0ac17e856ab2e760a568b7fc7ee19d6a18c20839580cc3d0daa3add1","kind":"log-pool-epochs","#,
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
    r#"{"diffId":"sha256:57cec0094bc6801fa606715cef6c5c05fb7c239bb22ac6c3943303c497679c42","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:c627c4c5b4502503b5ebccbce52584cbb7ee543e0ad162cc2e5d01d4c90d6777","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:732d42a28acbd3abd4bf3dfc1eb452fa332c371d8601e3c3d43797415692439f","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:c77ef149c9a351b638ec3a9d9f91ba498e19a44af6d3a72f8e6ee91385c760f4","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:9016a1c7d55c0a2523766dba66827d7b5c59ce1ba65068cc91c274f47f503b5f","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:a202395bb321da4047cbee71a851fc8a458f66a3094cebf9cb1ae8cdaf874805","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:df527420559c35e6714fac6464e604f33ee50d03fe9470bada31f71fb86ff574","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:e1b48405a880c4f338cd31b80f3ab62b489e163bc8af9bd7a7e5a823088e8444","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:83ebf9d14ea3125fe8e29597050d7bb5554cc56e0ae5452f403768637f954884","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:ef1df0567e7f9231763aae41feadd6608e63623a9e64c74e4f99323121cfe97f","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:82579f22d260a4cd7e45883a5e401d82726d053e1bf3e4098fea887fdf7a5d13","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:bb3dcdaca7726870ec256e018add03f04940ed1cdbc0094032bb7214dc22cab8","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:a99df1b648def4e9da03c594cd759ddbe197be1670ab8c4d4ac082555dec0b06","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:a661cf64259ef6ebe6071aa850c8a0b46ec1934db138d9d63c970fb60ae1ba90","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:f675fcca8599c8f334c9a8be5e2c2dc36e7ac6bfc9f1ab29238322d78e509a0f","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:0f4e2a3f2246f2e702d694c535e5eff6280ff4494fa1c14ec60f858cd724c94c","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:305c67536c0d380cc899ab04e6e177ed6571a776d9edbe37fe7f666a5ad3625a","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:fadd3f881ccb0868c8735094ce571df6153ec6b773cc9bfd0ea61cabf1a2240d","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:97b7f4dc88289d39230d36c3795783e2f5d6b2100a81482517ab57c31ee94eb2","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:cba0735fdec007e3d3ca320d3d0b713abd0cffccd6874ddb9d4d5a6cdc3e9a20","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:43648ba11c81dd241a4989e1c778ef6a47901ae1640c9787e6e4325ae74ed24f","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:b5a1ca751c3a1b42944e1ca32e6fc984bf7be04359f4bf3b216c3ac0d857b4a9","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:76e24229e77461a879b84e988831588366a413c2c8e249f8c8cacd35f32a72dc","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:8bbf2304394dc53255c4940b78b3c2eb02e470979246ccb399ec3ceec26c8df6","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:5db3baae6fc2c3f2296ae9dc35e2dfeef1fefc3dc6aa03cf4fde75d27ce9c67e","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:94e004d2d79e40e0ed338bff1243b738db98a8307274fe292c0baf8b8223f284","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"assets":1,"datums":1,"dreps":2,"epochs":3,"eras":2,"first-mint-chunks":2,"first-mints":2,"first-script-chunks":1,"first-scripts":1,"gov":4,"leader-rewards":0,"member-rewards":0,"metadata-labels":2,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":2,"proposals":4,"scripts":1,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-stats":1,"accounts":16,"address-stats":1,"asset-mints":1,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"scripts":16,"utxos":16},"#,
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
