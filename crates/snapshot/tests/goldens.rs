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
//! Between them these freeze: the twenty-eight media types, the tag string, the
//! twelve archive dimension names, the three exact-record kind literals, the
//! seven `log-{ns}` and eighteen `state-{ns}` kind strings — which is where the
//! twenty-five namespace strings live now that neither record names one — the
//! layer header and scope shapes, and the `position`/`parameters` key
//! spellings, the shard and schema maps included.

mod common;

use common::*;
use dolos_core::{BlockHash, ChainPoint};
use dolos_snapshot::{
    layers::{blocks, digests, indexes, logs, state},
    log_ns_for, state_ns_for, DolosProfile, BLOCKS, DIGESTS, INDEXES,
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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 32] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:557948cad9dec8605fbde96912db9b6421b47f2ded4c00886ed59f1638b4678c",
        16,
        431,
    ),
    (
        "log-account-epochs",
        "sha256:be63f933f87028f2650741a6964a8a0e1bae9a12f73e012f0854a4c568a23055",
        2,
        102,
    ),
    (
        "log-account-stakes",
        "sha256:65ebf2cf77a4f4ceb4af5a4cbdd0863ba24066a56e208a213285757f0147705b",
        2,
        102,
    ),
    (
        "log-epochs",
        "sha256:4e7e25d6fa1bbbbc6536270f6415009d9903ec3718a22a48a0b6b7752c3075ae",
        2,
        94,
    ),
    (
        "log-leader-rewards",
        "sha256:acc84431de8ded1d813137b7b2df43be2945323de0f1c0afb2d3ac51034f0141",
        2,
        102,
    ),
    (
        "log-member-rewards",
        "sha256:fe5236f16a7e4ddf174fc1f639b465dcbbf01127e9b555587e7b77b0ba328b88",
        2,
        102,
    ),
    (
        "log-pool-deposit-refunds",
        "sha256:c7d0678ad8095f97716409c61d671a20c22cc79dce89c9644a42a97196aaaa2c",
        2,
        109,
    ),
    (
        "log-stakes",
        "sha256:d35173ccd9c8de528db9de9884dd967bffdd6c6c5a823bae98d902aa279c7023",
        2,
        94,
    ),
    (
        "state-account-epochs",
        "sha256:9946fd4c26d9cdb01865650503f94bd2c1cf8367b4a40e3a8f7cb0d65f5718eb",
        2,
        93,
    ),
    (
        "state-account-stakes",
        "sha256:041f7ef9f6a9dd8a5a0570aec61eea58ee08d60692d316cf97f6f96ec1e513fc",
        2,
        93,
    ),
    (
        "state-accounts",
        "sha256:720c3a8b9f813e34659481f048131051ab6acd8458d88dac2777b026b0d22b6c",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:43efd9b02c4c4aad39ea8256e72ba5dcd92bb4fe435d68f757dd8f80ebfc4e14",
        2,
        87,
    ),
    (
        "state-assets",
        "sha256:1d481716b1e2791480da17f30cca1a6cbde12b1234dffe62dfa064463f383f86",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:d8c2636b8828766b63df422e1b354f834cae1720f8da297dc68166cfc0082908",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:74b761e5661a83be720df117c0e4da01fa334de06acf94611db52aaa1d154d8a",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:19e5b18452a6b8c06a7c10e246b5efa6a61181d2963767e1b63c9d3d62ab9200",
        2,
        85,
    ),
    (
        "state-dreps",
        "sha256:996990e3e1afe753eea5b77cf2c1951c145061e980212040854dccdcff9b769c",
        2,
        84,
    ),
    (
        "state-epochs",
        "sha256:7c65c4e3a9cd9640031acbafd4e1dc3afd1ca42e765c088a9bfd811c75811ca3",
        2,
        85,
    ),
    (
        "state-eras",
        "sha256:fe5ffade76fc5ff11adcf9e5c42b63af55ff473425d81915d593a4b9bed14eee",
        2,
        83,
    ),
    (
        "state-gov",
        "sha256:9e0fdf4787c4be1eba09110748602aab30e047f4ef0490d1b298d7505f518f5d",
        2,
        82,
    ),
    (
        "state-leader-rewards",
        "sha256:9a1decdcb2e7bb429c50369a992a2c5fbd736a303aa85c3c059fd29ebbf6cb76",
        2,
        93,
    ),
    (
        "state-member-rewards",
        "sha256:15fd4ddd9d5ad2d459a4cbebd9a3e013594e9a73df2e680c2b8d532d8db6a5d4",
        2,
        93,
    ),
    (
        "state-pending-mirs",
        "sha256:3550177cdb9520755eb859cee95a52529ec72aa57da6a57f088714ac547aa634",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:14ea25a953670e54cd27bc1236f8b532ad6ed6e2db82887edf4c4606e0b4b491",
        2,
        94,
    ),
    (
        "state-pool-deposit-refunds",
        "sha256:bbae5c7b47f1cd6b6be0691d8fe07134dda969564589e63bd579c0e5492c5bf8",
        2,
        100,
    ),
    (
        "state-pools",
        "sha256:b755896459f602734e4e307a56fe8bd090a330e9bd974d45f09cc1ef9475a7d7",
        2,
        84,
    ),
    (
        "state-proposals",
        "sha256:490178599b6c1113ac6bf2cfb6436006814d135101cedf12beea0b6ca1f2a303",
        2,
        88,
    ),
    (
        "state-stakes",
        "sha256:35f6e7db3cef234da52027f1778678580ff4a2779769f95c50263939b1213ed7",
        2,
        85,
    ),
    (
        "state-utxos",
        "sha256:c3eca9837ba1ad8e292721c823a5e886a8907bf082111b5544c42f43481e8475",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:2eed0e675d090d65b987da35dd0a2bf4962a434d534d33a51e2b6fde68875390",
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
    "sha256:17de634cd0a445cf4c78ad3de07a02bb01c2b7f4b543c185ddae44fa3d598362";

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

/// Write the whole fixture stele into `root`: thirty layers and an inscription.
fn write_stele(root: &std::path::Path) -> (Inscription, Digest) {
    let stele = SteleDir::create(root).unwrap();

    let point = ChainPoint::Specific(END_SLOT, BlockHash::new(POINT_HASH));

    let mut inscription = Inscription::new(
        &DolosProfile,
        EPOCH,
        dolos_snapshot::position(&network(), &point, EPOCH).unwrap(),
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
    let stele = SteleDir::open(first.path()).unwrap();
    let read = stele.read_inscription().unwrap();

    assert_eq!(read, inscription);
    assert_eq!(read.digest().unwrap(), digest);
    read.check_profile(&DolosProfile).unwrap();

    // Every layer streams back under the *default* record ceiling — the
    // confirmation `crates/stelae`'s streaming reader was left waiting for from
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
    r#"{"diffId":"sha256:557948cad9dec8605fbde96912db9b6421b47f2ded4c00886ed59f1638b4678c","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":16,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":431},"#,
    r#"{"diffId":"sha256:be63f933f87028f2650741a6964a8a0e1bae9a12f73e012f0854a4c568a23055","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:65ebf2cf77a4f4ceb4af5a4cbdd0863ba24066a56e208a213285757f0147705b","kind":"log-account-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-stakes.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:4e7e25d6fa1bbbbc6536270f6415009d9903ec3718a22a48a0b6b7752c3075ae","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:acc84431de8ded1d813137b7b2df43be2945323de0f1c0afb2d3ac51034f0141","kind":"log-leader-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.log-leader-rewards.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:fe5236f16a7e4ddf174fc1f639b465dcbbf01127e9b555587e7b77b0ba328b88","kind":"log-member-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.log-member-rewards.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:c7d0678ad8095f97716409c61d671a20c22cc79dce89c9644a42a97196aaaa2c","kind":"log-pool-deposit-refunds","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-deposit-refunds.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":109},"#,
    r#"{"diffId":"sha256:d35173ccd9c8de528db9de9884dd967bffdd6c6c5a823bae98d902aa279c7023","kind":"log-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.log-stakes.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:9946fd4c26d9cdb01865650503f94bd2c1cf8367b4a40e3a8f7cb0d65f5718eb","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:041f7ef9f6a9dd8a5a0570aec61eea58ee08d60692d316cf97f6f96ec1e513fc","kind":"state-account-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stakes.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:720c3a8b9f813e34659481f048131051ab6acd8458d88dac2777b026b0d22b6c","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:43efd9b02c4c4aad39ea8256e72ba5dcd92bb4fe435d68f757dd8f80ebfc4e14","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:1d481716b1e2791480da17f30cca1a6cbde12b1234dffe62dfa064463f383f86","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:d8c2636b8828766b63df422e1b354f834cae1720f8da297dc68166cfc0082908","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:74b761e5661a83be720df117c0e4da01fa334de06acf94611db52aaa1d154d8a","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:19e5b18452a6b8c06a7c10e246b5efa6a61181d2963767e1b63c9d3d62ab9200","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:996990e3e1afe753eea5b77cf2c1951c145061e980212040854dccdcff9b769c","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:7c65c4e3a9cd9640031acbafd4e1dc3afd1ca42e765c088a9bfd811c75811ca3","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:fe5ffade76fc5ff11adcf9e5c42b63af55ff473425d81915d593a4b9bed14eee","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":83},"#,
    r#"{"diffId":"sha256:9e0fdf4787c4be1eba09110748602aab30e047f4ef0490d1b298d7505f518f5d","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:9a1decdcb2e7bb429c50369a992a2c5fbd736a303aa85c3c059fd29ebbf6cb76","kind":"state-leader-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-leader-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:15fd4ddd9d5ad2d459a4cbebd9a3e013594e9a73df2e680c2b8d532d8db6a5d4","kind":"state-member-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-member-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:3550177cdb9520755eb859cee95a52529ec72aa57da6a57f088714ac547aa634","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:14ea25a953670e54cd27bc1236f8b532ad6ed6e2db82887edf4c4606e0b4b491","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:bbae5c7b47f1cd6b6be0691d8fe07134dda969564589e63bd579c0e5492c5bf8","kind":"state-pool-deposit-refunds","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pool-deposit-refunds.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:b755896459f602734e4e307a56fe8bd090a330e9bd974d45f09cc1ef9475a7d7","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:490178599b6c1113ac6bf2cfb6436006814d135101cedf12beea0b6ca1f2a303","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":88},"#,
    r#"{"diffId":"sha256:35f6e7db3cef234da52027f1778678580ff4a2779769f95c50263939b1213ed7","kind":"state-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c3eca9837ba1ad8e292721c823a5e886a8907bf082111b5544c42f43481e8475","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:2eed0e675d090d65b987da35dd0a2bf4962a434d534d33a51e2b6fde68875390","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-epochs":1,"account-stakes":1,"accounts":1,"assets":1,"datums":1,"dreps":1,"epochs":2,"eras":1,"gov":1,"leader-rewards":1,"member-rewards":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":1,"pools":1,"proposals":1,"stakes":1,"utxos":1},"#,
    r#""shards":{"account-epochs":1,"account-stakes":1,"accounts":16,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"gov":1,"leader-rewards":1,"member-rewards":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":1,"pools":1,"proposals":1,"stakes":1,"utxos":16}"#,
    r#"},"position":{"epoch":7,"network":{"magic":764824073,"name":"mainnet"},"point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":7}"#,
);
/// Decode one record with the codec its layer kind names, so the streaming pass
/// exercises every codec rather than only the framing underneath them.
fn decode_one(kind: &str, record: &[u8]) {
    // One codec for all six log kinds: they differ in which namespace they
    // carry, never in how a record is written.
    if log_ns_for(kind).is_some() {
        logs::decode(record).unwrap();

        return;
    }

    // And one codec for all eighteen state kinds, for the same reason — the
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
