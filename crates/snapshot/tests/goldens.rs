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
//! Between them these freeze: the twenty-six media types, the tag string, the
//! thirteen archive dimension names, the three exact-record kind literals, the
//! six `log-{ns}` and seventeen `state-{ns}` kind strings — which is where the
//! twenty-three namespace strings live now that neither record names one — the
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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 30] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:338a9f00c8616a966c2b1b14db9c269880fff108bfbf45436195ad39e24f7b77",
        17,
        461,
    ),
    (
        "log-account-stakes",
        "sha256:defd04bff2ccc95109a5c3c8155b0e5bb4ec42dd996939ca03db0d0db6c08869",
        2,
        102,
    ),
    (
        "log-epochs",
        "sha256:a42213234eaff408cfbf9cde9f43eb782b41726da3e144d497c576f4cabf37c4",
        2,
        94,
    ),
    (
        "log-leader-rewards",
        "sha256:0ac14c9545543b26c4c313ba6c7a97dad7d16eb69dc6879e4fef4adc3535f16d",
        2,
        102,
    ),
    (
        "log-member-rewards",
        "sha256:a0fcb7bb279d584ec384259d89950f683f4585a7fd64791bd4b9c7cd0f7cca3b",
        2,
        102,
    ),
    (
        "log-pool-deposit-refunds",
        "sha256:acb48fa1a3fe5539ba8c10e8f6db63a96b74fc2bc007eb49dcb3c020dc739d1a",
        2,
        109,
    ),
    (
        "log-stakes",
        "sha256:4a38c30b91b4bb4142721f10c74c2c2fbd9cbf5e1efb5a08eb2c330fd204d0bd",
        2,
        94,
    ),
    (
        "state-account-stakes",
        "sha256:1b30a5fef9ec458336cbde5a7aa80aaec76d0c5a8bf68b577152d0bd506be80b",
        2,
        93,
    ),
    (
        "state-accounts",
        "sha256:db47596747e81165ce2948f333cc6e6ab3c240f6973fb627df1d9b65dc50c8f2",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:0c7bc44f2f59b16a803369072e19011789cf4221dc94f409626e52d00f10eb1f",
        2,
        87,
    ),
    (
        "state-assets",
        "sha256:8077fd64690d954acc788df3b8046d151729ccd2b7d1dae42ed177f3931747aa",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:c7c1ff92712bfa3bff88b9ce2445bb3f4e55139ac5bc89c989f41188f894eeff",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:41c9a9cba05c8bfc0d7e345b820f62d4fc3e16fa5deef8ac224f9422ae1c3bb0",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:444ae56c151ffb66b8dd716e16b578d9ec4e339251ba24406d935b5214db27da",
        2,
        85,
    ),
    (
        "state-dreps",
        "sha256:96f0a867e25607b374f34830c4499ae735a860486b866e740998c781ef0f29ca",
        2,
        84,
    ),
    (
        "state-epochs",
        "sha256:93b0aab4fd80315a8bf106397e7575f58b031a8fe9680dac5d21bf84c880a23a",
        2,
        85,
    ),
    (
        "state-eras",
        "sha256:ad1c687b6b519d6d922764504a333d91733a98a251c606524532c6b066167648",
        2,
        83,
    ),
    (
        "state-gov",
        "sha256:f569b56280287b58f66dc13c1f4c0b2f6895befb01399ec6d2c73e13a8679a97",
        2,
        82,
    ),
    (
        "state-leader-rewards",
        "sha256:35d6299d7c44b696f5ff9bc4ba1ad9bf565ef7ef8044bb7a729a0b8acaf7cef4",
        2,
        93,
    ),
    (
        "state-member-rewards",
        "sha256:900e8cbd5e6e0e24342acda6ea98547a402eb7f7a06909d96cc8a3cbefbcb962",
        2,
        93,
    ),
    (
        "state-pending-mirs",
        "sha256:b0902d68fbcfeb299e56b72bf9de34299928d6a0d25501ceaa5161d209c5bdf6",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:14f3ba06ea6efef528513e8a515d7642a58de6745726358c43c7674585228b9c",
        2,
        94,
    ),
    (
        "state-pool-deposit-refunds",
        "sha256:d7d44918e486050d16ccd0453336949a94e04ac0f16d3e1be526bf83e51635c4",
        2,
        100,
    ),
    (
        "state-pools",
        "sha256:ed53688d4d705b0403a38874f710aa44aecb5b2746beba513db5773d28b500c7",
        2,
        84,
    ),
    (
        "state-proposals",
        "sha256:45f5f9c494e2ed8971b944e45d0cce187d9b08227589ef636a6974a054d1f4b6",
        2,
        88,
    ),
    (
        "state-stakes",
        "sha256:30d99b0aa88a7e8b35f1a6c1601535c157f2b5d42f50853391ee596fa5a7ef44",
        2,
        85,
    ),
    (
        "state-utxos",
        "sha256:2f55f54d9ad1e7bd1b4419c6508741347865d732aeb817f0785f4bf329f18d19",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:ce4874d77aeb1e3c553f88c59f010315038b0577302e1c2b8d518a3877b48371",
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
    "sha256:99e353e2b2226360f8b8e6ad7f24b6966fb2f78c0d49a776e3463ad76b3c58f7";

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
    r#"{"diffId":"sha256:338a9f00c8616a966c2b1b14db9c269880fff108bfbf45436195ad39e24f7b77","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":17,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":461},"#,
    r#"{"diffId":"sha256:defd04bff2ccc95109a5c3c8155b0e5bb4ec42dd996939ca03db0d0db6c08869","kind":"log-account-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-stakes.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:a42213234eaff408cfbf9cde9f43eb782b41726da3e144d497c576f4cabf37c4","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:0ac14c9545543b26c4c313ba6c7a97dad7d16eb69dc6879e4fef4adc3535f16d","kind":"log-leader-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.log-leader-rewards.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:a0fcb7bb279d584ec384259d89950f683f4585a7fd64791bd4b9c7cd0f7cca3b","kind":"log-member-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.log-member-rewards.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:acb48fa1a3fe5539ba8c10e8f6db63a96b74fc2bc007eb49dcb3c020dc739d1a","kind":"log-pool-deposit-refunds","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-deposit-refunds.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":109},"#,
    r#"{"diffId":"sha256:4a38c30b91b4bb4142721f10c74c2c2fbd9cbf5e1efb5a08eb2c330fd204d0bd","kind":"log-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.log-stakes.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:1b30a5fef9ec458336cbde5a7aa80aaec76d0c5a8bf68b577152d0bd506be80b","kind":"state-account-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stakes.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:db47596747e81165ce2948f333cc6e6ab3c240f6973fb627df1d9b65dc50c8f2","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:0c7bc44f2f59b16a803369072e19011789cf4221dc94f409626e52d00f10eb1f","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:8077fd64690d954acc788df3b8046d151729ccd2b7d1dae42ed177f3931747aa","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c7c1ff92712bfa3bff88b9ce2445bb3f4e55139ac5bc89c989f41188f894eeff","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:41c9a9cba05c8bfc0d7e345b820f62d4fc3e16fa5deef8ac224f9422ae1c3bb0","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:444ae56c151ffb66b8dd716e16b578d9ec4e339251ba24406d935b5214db27da","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:96f0a867e25607b374f34830c4499ae735a860486b866e740998c781ef0f29ca","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:93b0aab4fd80315a8bf106397e7575f58b031a8fe9680dac5d21bf84c880a23a","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:ad1c687b6b519d6d922764504a333d91733a98a251c606524532c6b066167648","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":83},"#,
    r#"{"diffId":"sha256:f569b56280287b58f66dc13c1f4c0b2f6895befb01399ec6d2c73e13a8679a97","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:35d6299d7c44b696f5ff9bc4ba1ad9bf565ef7ef8044bb7a729a0b8acaf7cef4","kind":"state-leader-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-leader-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:900e8cbd5e6e0e24342acda6ea98547a402eb7f7a06909d96cc8a3cbefbcb962","kind":"state-member-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-member-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:b0902d68fbcfeb299e56b72bf9de34299928d6a0d25501ceaa5161d209c5bdf6","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:14f3ba06ea6efef528513e8a515d7642a58de6745726358c43c7674585228b9c","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:d7d44918e486050d16ccd0453336949a94e04ac0f16d3e1be526bf83e51635c4","kind":"state-pool-deposit-refunds","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pool-deposit-refunds.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:ed53688d4d705b0403a38874f710aa44aecb5b2746beba513db5773d28b500c7","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:45f5f9c494e2ed8971b944e45d0cce187d9b08227589ef636a6974a054d1f4b6","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":88},"#,
    r#"{"diffId":"sha256:30d99b0aa88a7e8b35f1a6c1601535c157f2b5d42f50853391ee596fa5a7ef44","kind":"state-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:2f55f54d9ad1e7bd1b4419c6508741347865d732aeb817f0785f4bf329f18d19","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:ce4874d77aeb1e3c553f88c59f010315038b0577302e1c2b8d518a3877b48371","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-stakes":1,"accounts":1,"assets":1,"datums":1,"dreps":1,"epochs":1,"eras":1,"gov":1,"leader-rewards":1,"member-rewards":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":1,"pools":1,"proposals":1,"stakes":1,"utxos":1},"#,
    r#""shards":{"account-stakes":1,"accounts":16,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"gov":1,"leader-rewards":1,"member-rewards":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":1,"pools":1,"proposals":1,"stakes":1,"utxos":16}"#,
    r#"},"position":{"#,
    r#""epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":7}"#,
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

    // And one codec for all seventeen state kinds, for the same reason — the
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
