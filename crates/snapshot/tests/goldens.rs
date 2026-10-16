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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 68] = [
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
        "state-drep-sightings",
        "sha256:b600d226a541876fee5030871b7c7dea9e01888d2b0d82c3a7dbe462833f0c44",
        2,
        90,
    ),
    (
        "state-drep-sightings",
        "sha256:f89577e0d3665e43a9ff7b5aa68b1d8bdb16045e7ab130fa286f48414aaada68",
        2,
        90,
    ),
    (
        "state-dreps",
        "sha256:9c8976547370b9415eacccab4452a14963e48411828714dea7998e2f755baf9a",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:219ae8804aab0b163f153ec80114662e63320bbc2ed42f875da8a0d1573dbcd3",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:822d6309ccc9fe2f1feb832034900e9367a98cf087a258d78f369cbc04999ba1",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:d971b91625708237eddce3ab2ff841e3accb3c5fb9064b7dea4a1530c2959fe9",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:78b755926f0a052067d031fa74745e4580b9035c20103ca5bcee3846fb101e0f",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:351f28097d7632c6e56376bd839a2cbb796fb131083a2cef80e04af89ca206d3",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:91701aacedb5f29e872ad2571456104db4efe5cce60ab586d66dbf549baf3d07",
        2,
        126,
    ),
    (
        "state-first-mints",
        "sha256:ae6b93c38e2c139a8e3c581977f98dffa747e2b7dae4addb05f244c6571d22cf",
        2,
        126,
    ),
    (
        "state-first-scripts",
        "sha256:6a79143dd84d4bfa2e54a91aa084c28ff4fc8693626d13c0f0805650a4be6c27",
        2,
        96,
    ),
    (
        "state-first-scripts",
        "sha256:ac5b196c35ffc72f244d790aa18a20550610214116bd2cc9176892fc52e66a76",
        2,
        96,
    ),
    (
        "state-gov",
        "sha256:6d657c07b9f25e70efab48fe3d67263384ef4cc4c903e088f90b96d773570d4f",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:2998787a6a66686ab2589b3ca8216695e7f125af4d3eb126aa5bcb71ca1604c7",
        2,
        49,
    ),
    (
        "state-metadata-labels",
        "sha256:99751fb775b806ad332f8ab9a59996ab111efc79ef4dec0d8448a545cc4df70a",
        2,
        69,
    ),
    (
        "state-metadata-labels",
        "sha256:7e8bfb405b282d3ff72671b7e1262d23337c3ceb4d97abc78c3be26788ed1ca9",
        2,
        69,
    ),
    (
        "state-payment-addresses",
        "sha256:d8a997f34b8f16ac317eb686f7c50bbb850d2a86fa7a4c0f5b57c23c0e52a6a2",
        2,
        129,
    ),
    (
        "state-payment-addresses",
        "sha256:b7a65a46dbb23fe96e70f5dc3ebeefbe803f28978b4b06a6a309a11f73b1fd2b",
        2,
        129,
    ),
    (
        "state-payment-stats",
        "sha256:c2f19dad110fe08611805ff5de5bb361abc62031c5900e3b3df88576037c9323",
        2,
        96,
    ),
    (
        "state-payment-stats",
        "sha256:adbf78166c59129fc85b630fba7f8fd7041c08224164945416639b84ffa36b3b",
        2,
        96,
    ),
    (
        "state-pending-mirs",
        "sha256:93a358b72550a1306f3ed91399968572c417e62080e9ce055f179e9b982e9238",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:b185cdb1210a274a35261d89b6ee2993247dac1fb8ad4f356103f2140fde9980",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:9afa2d48f3d194819558535ae6200e00194ef51bc92ab187060b454ecf5b9a2f",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:2de4f70f0cab39095e0e6efccf0977c3b42e5217d2aa5f1578efab68000dbc56",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:a8c117ab2a29ba2d46984f16013159b0b9a06a0b8ad90be0676c228f8ff8c5d1",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:b3ad26557247426564f60f8d26ff635d23d855b8c5b4734f851672071caf1f24",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:7bcf16fda5c785456e9eeba7d94ab91476909a1e3fd3cf1e0fb1057804f1d7d3",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:9eb7709a0fbf4fe73f40f3a9ff0bbc7089084659da5e983c0dd709155627ff04",
        2,
        92,
    ),
    (
        "state-scripts",
        "sha256:cd8d55df5b59f760c61f5500a3bcdf773ca00968f7f90931765ba475a42ee640",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:c5341d7c2bcbe3ff25367255e78e29df11df022e930f6d973f0753496e521141",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:8f9d6003865a50de3f0656251e2842b2575acbd0550fc7e847663eea0db9a068",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:2464ba43f15698c4bd7fbc18c117a9a2fb807bdde1e60a364517e2eecbc26bf4",
        2,
        82,
    ),
    (
        "state-utxos",
        "sha256:c0e71f6801ef23c52e701b4d29df6d58ae8fa60effd13733f726be43570fbaa7",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:657fb8c96c49238b79bb48349af39432ea310f498c49c55aa5b076cf166d5d94",
        2,
        91,
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
    (
        DIGESTS,
        "sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80",
        3,
        250,
    ),
];

/// The stele's identity: sha256 of the canonical inscription.
const GOLDEN_INSCRIPTION: &str =
    "sha256:78d5db0542a7355f70b40ca5da506282f335f198145e71962488f27e637249d5";

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
    r#"{"diffId":"sha256:b600d226a541876fee5030871b7c7dea9e01888d2b0d82c3a7dbe462833f0c44","kind":"state-drep-sightings","#,
    r#""mediaType":"application/vnd.dolos.stele.state-drep-sightings.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:f89577e0d3665e43a9ff7b5aa68b1d8bdb16045e7ab130fa286f48414aaada68","kind":"state-drep-sightings","#,
    r#""mediaType":"application/vnd.dolos.stele.state-drep-sightings.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:9c8976547370b9415eacccab4452a14963e48411828714dea7998e2f755baf9a","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:219ae8804aab0b163f153ec80114662e63320bbc2ed42f875da8a0d1573dbcd3","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:822d6309ccc9fe2f1feb832034900e9367a98cf087a258d78f369cbc04999ba1","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:d971b91625708237eddce3ab2ff841e3accb3c5fb9064b7dea4a1530c2959fe9","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:78b755926f0a052067d031fa74745e4580b9035c20103ca5bcee3846fb101e0f","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:351f28097d7632c6e56376bd839a2cbb796fb131083a2cef80e04af89ca206d3","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:91701aacedb5f29e872ad2571456104db4efe5cce60ab586d66dbf549baf3d07","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:ae6b93c38e2c139a8e3c581977f98dffa747e2b7dae4addb05f244c6571d22cf","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:6a79143dd84d4bfa2e54a91aa084c28ff4fc8693626d13c0f0805650a4be6c27","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:ac5b196c35ffc72f244d790aa18a20550610214116bd2cc9176892fc52e66a76","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:6d657c07b9f25e70efab48fe3d67263384ef4cc4c903e088f90b96d773570d4f","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:2998787a6a66686ab2589b3ca8216695e7f125af4d3eb126aa5bcb71ca1604c7","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:99751fb775b806ad332f8ab9a59996ab111efc79ef4dec0d8448a545cc4df70a","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:7e8bfb405b282d3ff72671b7e1262d23337c3ceb4d97abc78c3be26788ed1ca9","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:d8a997f34b8f16ac317eb686f7c50bbb850d2a86fa7a4c0f5b57c23c0e52a6a2","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:b7a65a46dbb23fe96e70f5dc3ebeefbe803f28978b4b06a6a309a11f73b1fd2b","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:c2f19dad110fe08611805ff5de5bb361abc62031c5900e3b3df88576037c9323","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:adbf78166c59129fc85b630fba7f8fd7041c08224164945416639b84ffa36b3b","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:93a358b72550a1306f3ed91399968572c417e62080e9ce055f179e9b982e9238","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:b185cdb1210a274a35261d89b6ee2993247dac1fb8ad4f356103f2140fde9980","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:9afa2d48f3d194819558535ae6200e00194ef51bc92ab187060b454ecf5b9a2f","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:2de4f70f0cab39095e0e6efccf0977c3b42e5217d2aa5f1578efab68000dbc56","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:a8c117ab2a29ba2d46984f16013159b0b9a06a0b8ad90be0676c228f8ff8c5d1","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:b3ad26557247426564f60f8d26ff635d23d855b8c5b4734f851672071caf1f24","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:7bcf16fda5c785456e9eeba7d94ab91476909a1e3fd3cf1e0fb1057804f1d7d3","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:9eb7709a0fbf4fe73f40f3a9ff0bbc7089084659da5e983c0dd709155627ff04","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:cd8d55df5b59f760c61f5500a3bcdf773ca00968f7f90931765ba475a42ee640","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:c5341d7c2bcbe3ff25367255e78e29df11df022e930f6d973f0753496e521141","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:8f9d6003865a50de3f0656251e2842b2575acbd0550fc7e847663eea0db9a068","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:2464ba43f15698c4bd7fbc18c117a9a2fb807bdde1e60a364517e2eecbc26bf4","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:c0e71f6801ef23c52e701b4d29df6d58ae8fa60effd13733f726be43570fbaa7","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:657fb8c96c49238b79bb48349af39432ea310f498c49c55aa5b076cf166d5d94","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:61d7398a24f1c458b5030b44c5677229cef133ffb47c9b6416fecaaacb5adcd6","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:f193e457cf6094ca38250b65ce07d37be4e056b816106a56545d751d30bd8673","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"assets":1,"datums":1,"drep-sightings":1,"dreps":2,"epochs":3,"eras":2,"first-mint-chunks":2,"first-mints":2,"first-script-chunks":1,"first-scripts":1,"gov":4,"leader-rewards":0,"member-rewards":0,"metadata-labels":2,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":2,"proposals":4,"scripts":1,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-stats":1,"accounts":16,"address-stats":1,"asset-mints":1,"assets":16,"datums":16,"drep-sightings":1,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"scripts":16,"utxos":16},"#,
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
