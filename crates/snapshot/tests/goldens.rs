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
    layers::{blocks, digests, entities, indexes, logs, state},
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
        "sha256:fb039df612425645ba18b366a3625d4c41e2f1a8fb551b0609b131284ae3bda0",
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
        "log-payment-address-chunks",
        "sha256:e443537da6cd66935727db3732c0f64d94886e06b8fa9a54c761d4e86c9bb1d7",
        2,
        107,
    ),
    (
        "log-policy-mint-chunks",
        "sha256:bda33d8c36f56fd45f59a5611eab6b0b3e277c9eb2cd34e3a8a7761085267b94",
        2,
        106,
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
        "state-datums",
        "sha256:0e93eb46046670b85a194b8b3314f6ad6728eb260a8651282bf0b60fb0e5a1d6",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:44b5a0e88190a943b3e8a9a0967875ad2047482111465092f10a75f359131ead",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:4ce26949eeee213eb30d5e59aaa861f9bf07645273b3c1804616190216e27732",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:c687d070118b2b180595f2584e2551c88f5bd6a62d1c2e2891114e9a6f3be67b",
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
        "sha256:39ffab44309cc19ca16dd9197888f1fa25c6dfd1574aac4eebe89065d1480d81",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:7f88f3de0e71c3b8b8db721b4827aefcc2004d1d98f263200f5c8999da04edd8",
        2,
        92,
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
    "sha256:3be7bf59bc0e2225369931e3421cd0380ce96cde0a7dbe81f9c183933a7d1cff";

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
    r#"{"diffId":"sha256:fb039df612425645ba18b366a3625d4c41e2f1a8fb551b0609b131284ae3bda0","kind":"indexes","#,
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
    r#"{"diffId":"sha256:d649d6843908e3cb650b38066dd5b061d4adbe74cf17fe1a3c5498d43b196b67","kind":"log-drep-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-drep-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:9c0dff7fc49b26035e4f847167a57825e01a97d0c7e580375e4c7c9f5a2692e4","kind":"log-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.log-dreps.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:dfdbf6b422298a6d2fa090e36dfc5f48dd3527bfc6d7f3ee678644ec094a38c6","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:1e041ead8be23e9455adfe9a3b7f40bd304c63cc499e1929ecfb1cffb5952e92","kind":"log-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":76},"#,
    r#"{"diffId":"sha256:d54c77d975358c4e4461b1e5eb44f63fe2e474a77ad93601048cc41df1aaa8e0","kind":"log-first-script-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-script-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":78},"#,
    r#"{"diffId":"sha256:e443537da6cd66935727db3732c0f64d94886e06b8fa9a54c761d4e86c9bb1d7","kind":"log-payment-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-payment-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":107},"#,
    r#"{"diffId":"sha256:bda33d8c36f56fd45f59a5611eab6b0b3e277c9eb2cd34e3a8a7761085267b94","kind":"log-policy-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:586dc90b6000f08622a694aefefbcd9674aea59db02f94966fd2f7845bcc25ff","kind":"log-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":95},"#,
    r#"{"diffId":"sha256:b505113d38d258dcbd86078c80c7c7c0c080d3f9b542148f3150b5f3faa5bec9","kind":"log-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pools.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":89},"#,
    r#"{"diffId":"sha256:e4b121c8ea094b9e613bc8c38c0151289612456b454376e4722daa4091b39778","kind":"log-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.log-proposals.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":101},"#,
    r#"{"diffId":"sha256:b820744b499226137d7521dce605dae7c0f17fb1964dd5b8967d0ec2669a233c","kind":"entity-asset","#,
    r#""mediaType":"application/vnd.dolos.stele.entity-asset.v1+zstd","records":3,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":124},"#,
    r#"{"diffId":"sha256:ca4862a274c299824946a599d5d5468bd4944dbfabd396b767e83798ae5aae2b","kind":"entity-script","#,
    r#""mediaType":"application/vnd.dolos.stele.entity-script.v1+zstd","records":3,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":117},"#,
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
    r#"{"diffId":"sha256:0e93eb46046670b85a194b8b3314f6ad6728eb260a8651282bf0b60fb0e5a1d6","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:44b5a0e88190a943b3e8a9a0967875ad2047482111465092f10a75f359131ead","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:4ce26949eeee213eb30d5e59aaa861f9bf07645273b3c1804616190216e27732","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c687d070118b2b180595f2584e2551c88f5bd6a62d1c2e2891114e9a6f3be67b","kind":"state-datums","#,
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
    r#"{"diffId":"sha256:9016a1c7d55c0a2523766dba66827d7b5c59ce1ba65068cc91c274f47f503b5f","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:a202395bb321da4047cbee71a851fc8a458f66a3094cebf9cb1ae8cdaf874805","kind":"state-metadata-labels","#,
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
    r#"{"diffId":"sha256:39ffab44309cc19ca16dd9197888f1fa25c6dfd1574aac4eebe89065d1480d81","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:7f88f3de0e71c3b8b8db721b4827aefcc2004d1d98f263200f5c8999da04edd8","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
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
    r#""entities":{"asset":32,"script":28},"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"datums":1,"drep-epochs":1,"drep-sightings":1,"dreps":2,"epochs":4,"eras":2,"first-mint-chunks":2,"first-mints":2,"first-script-chunks":1,"first-scripts":1,"gov":4,"leader-rewards":0,"member-rewards":0,"metadata-labels":2,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":3,"proposals":4,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-stats":1,"accounts":16,"address-stats":1,"asset-mints":1,"datums":16,"drep-sightings":1,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"utxos":16},"#,
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
        DIGESTS => {
            digests::decode(record).unwrap();
        }
        other => panic!("no codec for layer kind {other:?}"),
    }
}
