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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 73] = [
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
        "log-epochs",
        "sha256:b6fe694d4e377c2c2925b66dfb4a45b464c53b3e9e2cd1410072865b760e7234",
        2,
        61,
    ),
    (
        "log-first-mint-chunks",
        "sha256:070aa9faa69bb572d7fc873b4aed838f81843f01677883f35aed23edc7a9de1a",
        2,
        76,
    ),
    (
        "log-first-script-chunks",
        "sha256:f62ef5f8ab636410b74e2339afce54e9ac030e15004b6d7ef82152730f01f933",
        2,
        78,
    ),
    (
        "log-payment-address-chunks",
        "sha256:75c0a0ab628a7b22586b715775cc9edffbf6884ae08177c83c017a59a7a2c346",
        2,
        107,
    ),
    (
        "log-policy-mint-chunks",
        "sha256:eecc1754f44e3575d4bbbd6e0b7aa55111b65e655f033d7240be3cfde6cd6fcf",
        2,
        106,
    ),
    (
        "log-pool-epochs",
        "sha256:c02fef757b3b8f5a65c04642cdc1e9c11a37f817528e43dcac261fbc983031a1",
        2,
        95,
    ),
    (
        "log-pools",
        "sha256:8cad266834a870a8f4c14a59a9ed20fe979e85cca819f9020f67252c2d54ac35",
        2,
        89,
    ),
    (
        "log-proposals",
        "sha256:bb6566fcadc86f92be8168fead3bfc57906caa9b2e085ab834ffa87ec68608f5",
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
        "sha256:1d454d559b886d60b556abfc0ffb5723b8acd92f74ac08917a6099b8188aaadd",
        2,
        90,
    ),
    (
        "state-drep-sightings",
        "sha256:4a5c2438d61f2f5ae9dc385ce5f9004aa2de8ca8a81d2fce0ed95f08ae16809d",
        2,
        90,
    ),
    (
        "state-dreps",
        "sha256:551848fb7c136cca366bfd8e76040bac90b28a315a928713cb841f443f531491",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:65344d61b41dbba4434cb79c6b6bdbb5043bd01b6b4c7486f080d944cc60029a",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:c1771bfc8542e6bdc0d40e96c089ee7b5ced9b4baf7eec87ceaab36afd54b240",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:46ccba5a377265c232d395c5f1743992eb157dfc7993a0a8ec2c6845cb46fbf7",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:c5fd885c371e58c40507e6eff70f9eb070046b01d6f5697869f35cc8379579c7",
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
        "sha256:94b89268cdf55f473af5e917975959b43bde1b891deb0f92bfb98b82ca8ff16d",
        2,
        126,
    ),
    (
        "state-first-mints",
        "sha256:04ef916b6323f686282d603a8edf0f988035390352002e07963f0e19030e96a4",
        2,
        126,
    ),
    (
        "state-first-scripts",
        "sha256:e1bb5d99ddf8da17f68129f071c768c9e3248e02e466ff2ac0d5dfbfccaa1769",
        2,
        96,
    ),
    (
        "state-first-scripts",
        "sha256:10ff2257802ddb96381112aa569bac0c82fdb26f323468aeec5ffb9514ee1391",
        2,
        96,
    ),
    (
        "state-gov",
        "sha256:bb2573643fc2cfaa7a7572e744d8b9fb9211f894d1382a7b31e4c4bdb364cb9c",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:c1614f8eeeda9390316ecb7fe40527a4c84e8eec01ee54a71dce70bb443b1c23",
        2,
        49,
    ),
    (
        "state-metadata-labels",
        "sha256:d1b049726cc7dd8d29970aadc14c5acb77f9e2b9b95192c0f215160308b37246",
        2,
        69,
    ),
    (
        "state-metadata-labels",
        "sha256:1edcdd20e01a63f8914b355e00ca88d9634a917148c3c4ae62ec3cd3c1c2adea",
        2,
        69,
    ),
    (
        "state-payment-addresses",
        "sha256:7a4e13103ac0a0e27c68e99003ff194cd88a41976575b3f4f46883744fcd1fb2",
        2,
        129,
    ),
    (
        "state-payment-addresses",
        "sha256:e8dc2798d9e16d7d7cf951ecad06f272c051c989f00a45d76d581611ce73769a",
        2,
        129,
    ),
    (
        "state-payment-stats",
        "sha256:d314a74345438deffc1e13e8987318a70a179815d3a7418cd576aad7bd40cafe",
        2,
        96,
    ),
    (
        "state-payment-stats",
        "sha256:f1e14f31961be425a1699e499a9295767a2d9265c5c2afc0830e70b1845d5508",
        2,
        96,
    ),
    (
        "state-pending-mirs",
        "sha256:e417ae2faa9f3dcfe3f62b80e8d5ad5ac25b41b738383358c518887702563d03",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:d679db631912bbcc0e3677a8030d03e500cd431095c127892b21677c66d056d5",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:71aa41dfb750773162dd7772afa87eb3324b950b4309f0ee3251243b01a02ab8",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:39fa85f5115b7e8ef7a2a839c3e06864d6751f61a647db46074da658d8a13e0c",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:6d7073e18f52f06db36118ece39733e2a472a7f0297b8881bc62c553404a7625",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:2e08f9652e630bb56ccebab9fa7b9a718803b001a1e701daee0fc2f98386447d",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:d3c5a0e174489d9a8522624369f38f3505ce8deb89e68d519fccbe2ff51a54a9",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:b2b0cd4830b6717c2560c00186b083b16cf9aab342528d3382a575aac1c83859",
        2,
        92,
    ),
    (
        "state-scripts",
        "sha256:3b078fcf6aff66fc64dda5ef1b993c39a162a965b0563de7d2abd19097564764",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:18e9bc4720fac9f4e63d9322ec6efcbbbe8dc2ddf85ad6b3be9446698d04a09d",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:1fa444460331e9e9399f036174d96f8471995d762a9675de125f8a22fa1065b5",
        2,
        82,
    ),
    (
        "state-scripts",
        "sha256:9bf20842b56b88a2dc94e4d1e20e05ca3e1cef59acd151b63d219a07559258c2",
        2,
        82,
    ),
    (
        "state-utxos",
        "sha256:b66979ba19b82c97de5031b923a09c8ebc26b4ba9d4d45477a84a905a2f6a752",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:c532bceb633a91dd6cc0b8789cf3170a55aaa6676b37229ed399bdc36378ab21",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:b882014a52055e12c9b4f1ef7c1aeddbdac5e4887146aab3b5b9f20f68b50d45",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:5b863d374031f390aa04299fc4b3527258401e5d438f1d6e422539aad0e29244",
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
    "sha256:0c070263320088b592e09a8f0f47e0122988a07753be4615e5d86c8c1e31dfa6";

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
    r#"{"diffId":"sha256:b6fe694d4e377c2c2925b66dfb4a45b464c53b3e9e2cd1410072865b760e7234","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:070aa9faa69bb572d7fc873b4aed838f81843f01677883f35aed23edc7a9de1a","kind":"log-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":76},"#,
    r#"{"diffId":"sha256:f62ef5f8ab636410b74e2339afce54e9ac030e15004b6d7ef82152730f01f933","kind":"log-first-script-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-script-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":78},"#,
    r#"{"diffId":"sha256:75c0a0ab628a7b22586b715775cc9edffbf6884ae08177c83c017a59a7a2c346","kind":"log-payment-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-payment-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":107},"#,
    r#"{"diffId":"sha256:eecc1754f44e3575d4bbbd6e0b7aa55111b65e655f033d7240be3cfde6cd6fcf","kind":"log-policy-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:c02fef757b3b8f5a65c04642cdc1e9c11a37f817528e43dcac261fbc983031a1","kind":"log-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":95},"#,
    r#"{"diffId":"sha256:8cad266834a870a8f4c14a59a9ed20fe979e85cca819f9020f67252c2d54ac35","kind":"log-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pools.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":89},"#,
    r#"{"diffId":"sha256:bb6566fcadc86f92be8168fead3bfc57906caa9b2e085ab834ffa87ec68608f5","kind":"log-proposals","#,
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
    r#"{"diffId":"sha256:1d454d559b886d60b556abfc0ffb5723b8acd92f74ac08917a6099b8188aaadd","kind":"state-drep-sightings","#,
    r#""mediaType":"application/vnd.dolos.stele.state-drep-sightings.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:4a5c2438d61f2f5ae9dc385ce5f9004aa2de8ca8a81d2fce0ed95f08ae16809d","kind":"state-drep-sightings","#,
    r#""mediaType":"application/vnd.dolos.stele.state-drep-sightings.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:551848fb7c136cca366bfd8e76040bac90b28a315a928713cb841f443f531491","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:65344d61b41dbba4434cb79c6b6bdbb5043bd01b6b4c7486f080d944cc60029a","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:c1771bfc8542e6bdc0d40e96c089ee7b5ced9b4baf7eec87ceaab36afd54b240","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:46ccba5a377265c232d395c5f1743992eb157dfc7993a0a8ec2c6845cb46fbf7","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:c5fd885c371e58c40507e6eff70f9eb070046b01d6f5697869f35cc8379579c7","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:1428f74ac30be5b84f17937afb1f41cbf30c4677698b7c3686afee195f696da3","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:94b89268cdf55f473af5e917975959b43bde1b891deb0f92bfb98b82ca8ff16d","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:04ef916b6323f686282d603a8edf0f988035390352002e07963f0e19030e96a4","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:e1bb5d99ddf8da17f68129f071c768c9e3248e02e466ff2ac0d5dfbfccaa1769","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:10ff2257802ddb96381112aa569bac0c82fdb26f323468aeec5ffb9514ee1391","kind":"state-first-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:bb2573643fc2cfaa7a7572e744d8b9fb9211f894d1382a7b31e4c4bdb364cb9c","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:c1614f8eeeda9390316ecb7fe40527a4c84e8eec01ee54a71dce70bb443b1c23","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:d1b049726cc7dd8d29970aadc14c5acb77f9e2b9b95192c0f215160308b37246","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:1edcdd20e01a63f8914b355e00ca88d9634a917148c3c4ae62ec3cd3c1c2adea","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":69},"#,
    r#"{"diffId":"sha256:7a4e13103ac0a0e27c68e99003ff194cd88a41976575b3f4f46883744fcd1fb2","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:e8dc2798d9e16d7d7cf951ecad06f272c051c989f00a45d76d581611ce73769a","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:d314a74345438deffc1e13e8987318a70a179815d3a7418cd576aad7bd40cafe","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:f1e14f31961be425a1699e499a9295767a2d9265c5c2afc0830e70b1845d5508","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:e417ae2faa9f3dcfe3f62b80e8d5ad5ac25b41b738383358c518887702563d03","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:d679db631912bbcc0e3677a8030d03e500cd431095c127892b21677c66d056d5","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:71aa41dfb750773162dd7772afa87eb3324b950b4309f0ee3251243b01a02ab8","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:39fa85f5115b7e8ef7a2a839c3e06864d6751f61a647db46074da658d8a13e0c","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:6d7073e18f52f06db36118ece39733e2a472a7f0297b8881bc62c553404a7625","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:2e08f9652e630bb56ccebab9fa7b9a718803b001a1e701daee0fc2f98386447d","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:d3c5a0e174489d9a8522624369f38f3505ce8deb89e68d519fccbe2ff51a54a9","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:b2b0cd4830b6717c2560c00186b083b16cf9aab342528d3382a575aac1c83859","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:3b078fcf6aff66fc64dda5ef1b993c39a162a965b0563de7d2abd19097564764","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:18e9bc4720fac9f4e63d9322ec6efcbbbe8dc2ddf85ad6b3be9446698d04a09d","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:1fa444460331e9e9399f036174d96f8471995d762a9675de125f8a22fa1065b5","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:9bf20842b56b88a2dc94e4d1e20e05ca3e1cef59acd151b63d219a07559258c2","kind":"state-scripts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-scripts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:b66979ba19b82c97de5031b923a09c8ebc26b4ba9d4d45477a84a905a2f6a752","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:c532bceb633a91dd6cc0b8789cf3170a55aaa6676b37229ed399bdc36378ab21","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:b882014a52055e12c9b4f1ef7c1aeddbdac5e4887146aab3b5b9f20f68b50d45","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:5b863d374031f390aa04299fc4b3527258401e5d438f1d6e422539aad0e29244","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""entities":{"asset":32,"script":28},"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"assets":1,"datums":1,"drep-epochs":1,"drep-sightings":1,"dreps":2,"epochs":4,"eras":2,"first-mint-chunks":2,"first-mints":2,"first-script-chunks":1,"first-scripts":1,"gov":4,"leader-rewards":0,"member-rewards":0,"metadata-labels":2,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":3,"proposals":4,"scripts":1,"utxos":1},"#,
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
